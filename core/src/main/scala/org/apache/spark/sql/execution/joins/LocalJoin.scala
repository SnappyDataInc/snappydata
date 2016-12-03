/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.execution.joins

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Callable, ExecutionException, TimeUnit}

import scala.collection.mutable

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import io.snappydata.Constant

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.{BindReferences, BoundReference, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, ObjectHashMapAccessor, ObjectHashSet, RowTableScan, SparkPlan}
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{SnappyAggregation, SnappySession}
import org.apache.spark.{Partition, SparkEnv, TaskContext}

/**
 * :: DeveloperApi ::
 * Performs an local hash join of two child relations. If a relation
 * (out of a datasource) is already replicated accross all nodes then rather
 * than doing a Broadcast join which can be expensive, this join just
 * scans through the single partition of the replicated relation while
 * streaming through the other relation.
 */
@DeveloperApi
case class LocalJoin(leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    condition: Option[Expression],
    joinType: JoinType,
    left: SparkPlan,
    right: SparkPlan)
    extends BinaryExecNode with HashJoin with CodegenSupport {

  @transient private var mapAccessor: ObjectHashMapAccessor = _
  @transient private var hashMapTerm: String = _
  @transient private var mapDataTerm: String = _
  @transient private var maskTerm: String = _
  @transient private var keyIsUniqueTerm: String = _
  @transient private var numRowsTerm: String = _

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  protected lazy val (buildSideKeys, streamSideKeys) = {
    require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    buildSide match {
      case BuildLeft => (leftKeys, rightKeys)
      case BuildRight => (rightKeys, leftKeys)
    }
  }

  private lazy val streamRDD = streamedPlan.execute()
  private lazy val (buildRDD, buildPartition) = {
    val rdd = buildPlan.execute()
    assert(rdd.getNumPartitions == 1)
    (rdd, rdd.partitions(0))
  }

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
   */
  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val buildDataSize = longMetric("buildDataSize")
    val buildTime = longMetric("buildTime")

    // materialize dependencies in the entire buildRDD graph for
    // buildRDD.iterator to work in the compute of mapPartitionsPreserve below
    materializeDependencies(buildRDD, new mutable.HashSet[RDD[_]]())

    val schema = buildPlan.schema
    streamRDD.mapPartitionsPreserveWithPartition { (context, split, itr) =>
      val start = System.nanoTime()
      val hashed = HashedRelationCache.get(schema, buildKeys, buildRDD,
        buildPartition, context)
      buildTime += (System.nanoTime() - start) / 1000000L
      val estimatedSize = hashed.estimatedSize
      buildDataSize += estimatedSize
      context.taskMetrics().incPeakExecutionMemory(estimatedSize)
      context.addTaskCompletionListener(_ => hashed.close())
      join(itr, hashed, numOutputRows)
    }
  }

  private[spark] def materializeDependencies[T](rdd: RDD[T],
      visited: mutable.HashSet[RDD[_]]): Unit = {
    rdd.dependencies.foreach(dep =>
      if (visited.add(dep.rdd)) materializeDependencies(dep.rdd, visited))
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()

  override def doProduce(ctx: CodegenContext): String = {
    if (SnappyAggregation.enableOptimizedAggregation) doProduceOptimized(ctx)
    else {
      streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)
    }
  }

  private def doProduceOptimized(ctx: CodegenContext): String = {
    val initMap = ctx.freshName("initMap")
    ctx.addMutableState("boolean", initMap, s"$initMap = false;")

    val createMap = ctx.freshName("createMap")

    // generate variable name for hash map for use here and in consume
    hashMapTerm = ctx.freshName("hashMap")
    val hashSetClassName = classOf[ObjectHashSet[_]].getName
    ctx.addMutableState(hashSetClassName, hashMapTerm, "")

    // add reference to the row table RDD directly since it is not possible
    // to pass to inputRDDs in the general case (when streamPlan already
    //   has 2 RDDs)
    val rowIterator = ctx.freshName("rowIterator")
    // find the underlying RowTableScan and set it up to use its RDD's direct
    // iterator for best performance
    val rowTableScan = buildPlan.collectFirst {
      case scan: RowTableScan if scan.numBuckets == 1 =>
        scan.input = rowIterator; scan
    }.getOrElse(throw new IllegalStateException(
      s"Failed to find replicated table for LocalJoin in $buildPlan"))
    val rdd = rowTableScan.dataRDD
    assert(rdd.getNumPartitions == 1)
    val rowTableRDD = ctx.addReferenceObj("rowTableRDD", rdd)
    val rowTablePart = ctx.addReferenceObj("singlePartition", rdd.partitions(0))

    // generate local variables for HashMap data array and mask
    mapDataTerm = ctx.freshName("mapData")
    maskTerm = ctx.freshName("hashMapMask")
    keyIsUniqueTerm = ctx.freshName("keyIsUnique")
    numRowsTerm = ctx.freshName("numRows")
    // generate the map accessor to generate key/value class
    // and get map access methods
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    mapAccessor = ObjectHashMapAccessor(session, ctx, buildSideKeys,
      buildPlan.output, "LocalMap", hashMapTerm, mapDataTerm, maskTerm,
      multiMap = true, this, this.parent, buildPlan)

    val entryClass = mapAccessor.getClassName
    val numKeyColumns = buildSideKeys.length
    ctx.addMutableState("scala.collection.Iterator", rowIterator,
      s"this.$rowIterator = $rowTableRDD.iterator($rowTablePart, " +
          "org.apache.spark.TaskContext.get());")
    ctx.addNewFunction(createMap,
      s"""
        private void $createMap() throws java.io.IOException {
          $hashMapTerm = new $hashSetClassName(128, 0.6, $numKeyColumns,
            scala.reflect.ClassTag$$.MODULE$$.apply($entryClass.class));
          int $maskTerm = $hashMapTerm.mask();
          $entryClass[] $mapDataTerm = ($entryClass[])$hashMapTerm.data();

          ${buildPlan.asInstanceOf[CodegenSupport].produce(ctx, mapAccessor)}
        }
       """)

    // clear the parent by reflection if plan is sent by operators like Sort
    val parentSetter = buildPlan.getClass.getMethod("parent_$eq",
      classOf[CodegenSupport])
    parentSetter.setAccessible(true)
    parentSetter.invoke(buildPlan, null)

    // clear the input of RowTableScan
    rowTableScan.input = null

    // The child could change `copyResult` to true, but we had already
    // consumed all the rows, so `copyResult` should be reset to `false`.
    ctx.copyResult = false

    val buildTime = metricTerm(ctx, "buildTime")
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    // initialization of min/max for integral keys
    val initMinMaxVars = mapAccessor.integralKeys.map { index =>
      val minVar = mapAccessor.integralKeysMinVars(index)
      val maxVar = mapAccessor.integralKeysMaxVars(index)
      s"""
        final long $minVar = $hashMapTerm.getMinValue($index);
        final long $maxVar = $hashMapTerm.getMaxValue($index);
      """
    }.mkString("\n")

    s"""
      boolean $keyIsUniqueTerm = true;
      if (!$initMap) {
        final long beforeMap = System.nanoTime();
        $createMap();
        $keyIsUniqueTerm = $hashMapTerm.keyIsUnique();
        $buildTime.add((System.nanoTime() - beforeMap) / 1000000);
        $initMap = true;
      }
      $initMinMaxVars
      final int $maskTerm = $hashMapTerm.mask();
      final $entryClass[] $mapDataTerm = ($entryClass[])$hashMapTerm.data();
      long $numRowsTerm = 0L;
      try {
        ${streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)}
      } finally {
        $numOutputRows.add($numRowsTerm);
      }
    """
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    if (SnappyAggregation.enableOptimizedAggregation) {
      return doConsumeOptimized(ctx, input)
    }
    // create a name for HashedRelation
    val relationTerm = ctx.freshName("relation")
    val relationIsUnique = ctx.freshName("keyIsUnique")
    val buildRDDRef = ctx.addReferenceObj("buildRDD", buildRDD)
    val buildPartRef = ctx.addReferenceObj("buildPartition", buildPartition)
    ctx.addMutableState(classOf[HashedRelation].getName, relationTerm,
      prepareHashedRelation(ctx, relationTerm, buildRDDRef, buildPartRef))
    ctx.addMutableState(classOf[Boolean].getName, relationIsUnique,
      s"$relationIsUnique = $relationTerm.keyIsUnique();")

    joinType match {
      case Inner => codeGenInner(ctx, input, relationTerm, relationIsUnique)
      case LeftOuter | RightOuter => codeGenOuter(ctx, input,
        relationTerm, relationIsUnique)
      case LeftSemi => codeGenSemi(ctx, input, relationTerm, relationIsUnique)
      case LeftAnti => codeGenAnti(ctx, input, relationTerm, relationIsUnique)
      case j: ExistenceJoin => codeGenExistence(ctx, input,
        relationTerm, relationIsUnique)
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastHashJoin should not take $x as the JoinType")
    }
  }

  private def doConsumeOptimized(ctx: CodegenContext,
      input: Seq[ExprCode]): String = {
    // variable that holds if relation is unique to optimize iteration
    val entryVar = ctx.freshName("entry")
    val localValueVar = ctx.freshName("value")
    val checkNullObj = joinType match {
      case LeftOuter | RightOuter | FullOuter => true
      case _ => false
    }
    val (initCode, keyValueVars, nullMaskVars) = mapAccessor.getColumnVars(
      entryVar, localValueVar, onlyKeyVars = false, onlyValueVars = false,
      checkNullObj)
    val buildKeyVars = keyValueVars.take(buildSideKeys.length)
    val buildVars = keyValueVars.drop(buildSideKeys.length)
    val checkCondition = getJoinCondition(ctx, input, buildVars)

    ctx.INPUT_ROW = null
    ctx.currentVars = input
    val (resultVars, streamKeys) = buildSide match {
      case BuildLeft => (buildVars ++ input,
          streamSideKeys.map(BindReferences.bindReference(_, right.output)))
      case BuildRight => (input ++ buildVars,
          streamSideKeys.map(BindReferences.bindReference(_, left.output)))
    }
    val streamKeyVars = ctx.generateExpressions(streamKeys)
    mapAccessor.generateMapLookup(entryVar, localValueVar, keyIsUniqueTerm,
      numRowsTerm, nullMaskVars, initCode, checkCondition,
      streamSideKeys, streamKeyVars, buildKeyVars, buildVars, input,
      resultVars, joinType)
  }

  /**
   * Returns code for creating a HashedRelation.
   */
  private def prepareHashedRelation(ctx: CodegenContext,
      relationTerm: String, buildRDDTerm: String,
      buildPartTerm: String): String = {
    val startName = ctx.freshName("start")
    val sizeName = ctx.freshName("estimatedSize")
    val contextName = ctx.freshName("context")
    val buildKeysVar = ctx.addReferenceObj("buildKeys", buildKeys)
    val buildSchemaVar = ctx.addReferenceObj("buildSchema", buildPlan.schema)
    val buildDataSize = metricTerm(ctx, "buildDataSize")
    val buildTime = metricTerm(ctx, "buildTime")
    s"""
       |final long $startName = System.nanoTime();
       |final org.apache.spark.TaskContext $contextName =
       |  org.apache.spark.TaskContext.get();
       |$relationTerm = org.apache.spark.sql.execution.joins.HashedRelationCache
       |  .get($buildSchemaVar, $buildKeysVar, $buildRDDTerm, $buildPartTerm,
       |  $contextName, 1);
       |$buildTime.add((System.nanoTime() - $startName) / 1000000L);
       |final long $sizeName = $relationTerm.estimatedSize();
       |$buildDataSize.add($sizeName);
       |$contextName.taskMetrics().incPeakExecutionMemory($sizeName);
    """.stripMargin
  }

  /**
   * Returns the code for generating join key for stream side,
   * and expression of whether the key has any null in it or not.
   */
  private def genStreamSideJoinKey(
      ctx: CodegenContext,
      input: Seq[ExprCode]): (ExprCode, String) = {
    ctx.currentVars = input
    if (streamedKeys.length == 1 && streamedKeys.head.dataType == LongType) {
      // generate the join key as Long
      val ev = streamedKeys.head.genCode(ctx)
      (ev, ev.isNull)
    } else {
      // generate the join key as UnsafeRow
      val ev = GenerateUnsafeProjection.createCode(ctx, streamedKeys)
      (ev, s"${ev.value}.anyNull()")
    }
  }

  /**
   * Generates the code for variable of build side.
   */
  private def genBuildSideVars(ctx: CodegenContext,
      matched: String): Seq[ExprCode] = {
    ctx.currentVars = null
    ctx.INPUT_ROW = matched
    buildPlan.output.zipWithIndex.map { case (a, i) =>
      val ev = BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      if (joinType == Inner) {
        ev
      } else {
        // the variables are needed even there is no matched rows
        val isNull = ctx.freshName("isNull")
        val value = ctx.freshName("value")
        val code =
          s"""
             |boolean $isNull = true;
             |${ctx.javaType(a.dataType)} $value =
             |  ${ctx.defaultValue(a.dataType)};
             |if ($matched != null) {
             |  ${ev.code}
             |  $isNull = ${ev.isNull};
             |  $value = ${ev.value};
             |}
         """.stripMargin
        ExprCode(code, isNull, value)
      }
    }
  }

  /**
   * Generate the (non-equi) condition used to filter joined rows.
   * This is used in Inner joins.
   */
  private def getJoinCondition(ctx: CodegenContext,
      input: Seq[ExprCode],
      buildVars: Seq[ExprCode]): Option[ExprCode] = condition match {
    case Some(expr) =>
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars,
        expr.references)
      // filter the output via condition
      ctx.currentVars = input ++ buildVars
      val ev = BindReferences.bindReference(expr,
        streamedPlan.output ++ buildPlan.output).genCode(ctx)
      Some(ev.copy(code =
          s"""
            $eval
            ${ev.code}
          """))
    case None => None
  }

  /**
   * Generate the (non-equi) condition used to filter joined rows.
   * This is used in Inner, Left Semi and Left Anti joins.
   */
  private def getJoinCondition(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      anti: Boolean = false): (String, String, String, Seq[ExprCode]) = {
    val matched = ctx.freshName("matched")
    val buildVars = genBuildSideVars(ctx, matched)
    val (checkCondition, antiCondition) = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars,
        expr.references)
      // filter the output via condition
      ctx.currentVars = input ++ buildVars
      val ev = BindReferences.bindReference(expr,
        streamedPlan.output ++ buildPlan.output).genCode(ctx)
      val cond =
        s"""
           |$eval
           |${ev.code}
           |if (${ev.isNull} || !${ev.value}) continue;
        """.stripMargin
      val antiCond = if (anti) {
        s"""
           |$eval
           |${ev.code}
           |if (!${ev.isNull} && ${ev.value}) continue;
        """.stripMargin
      } else ""
      (cond, antiCond)
    } else {
      ("", "continue;")
    }
    (matched, checkCondition, antiCondition, buildVars)
  }

  /**
   * Generates the code for Inner join.
   */
  private def codeGenInner(ctx: CodegenContext,
      input: Seq[ExprCode], relationTerm: String,
      relationIsUnique: String): String = {
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, _, buildVars) = getJoinCondition(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }
    ctx.copyResult = true
    val matches = ctx.freshName("matches")
    val iteratorCls = classOf[Iterator[UnsafeRow]].getName
    val consumeResult = consume(ctx, resultVars)

    s"""
       |if ($relationIsUnique) {
       |  // generate join key for stream side
       |  ${keyEv.code}
       |  // find matches from HashedRelation
       |  UnsafeRow $matched = $anyNull ? null : (UnsafeRow)$relationTerm
       |    .getValue(${keyEv.value});
       |  if ($matched == null) continue;
       |  $checkCondition
       |  $numOutput.add(1);
       |  $consumeResult
       |} else {
       |  // generate join key for stream side
       |  ${keyEv.code}
       |  // find matches from HashRelation
       |  $iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm
       |    .get(${keyEv.value});
       |  if ($matches == null) continue;
       |  while ($matches.hasNext()) {
       |    UnsafeRow $matched = (UnsafeRow)$matches.next();
       |    $checkCondition
       |    $numOutput.add(1);
       |    $consumeResult
       |  }
       |}
     """.stripMargin
  }

  /**
   * Generates the code for left or right outer join.
   */
  private def codeGenOuter(ctx: CodegenContext,
      input: Seq[ExprCode], relationTerm: String,
      relationIsUnique: String): String = {
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val matched = ctx.freshName("matched")
    val buildVars = genBuildSideVars(ctx, matched)
    val numOutput = metricTerm(ctx, "numOutputRows")

    // filter the output via condition
    val conditionPassed = ctx.freshName("conditionPassed")
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars,
        expr.references)
      ctx.currentVars = input ++ buildVars
      val ev = BindReferences.bindReference(expr,
        streamedPlan.output ++ buildPlan.output).genCode(ctx)
      s"""
         |boolean $conditionPassed = true;
         |${eval.trim}
         |${ev.code}
         |if ($matched != null) {
         |  $conditionPassed = !${ev.isNull} && ${ev.value};
         |}
       """.stripMargin.trim
    } else {
      s"final boolean $conditionPassed = true;"
    }

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }
    ctx.copyResult = true
    val matches = ctx.freshName("matches")
    val iteratorCls = classOf[Iterator[UnsafeRow]].getName
    val found = ctx.freshName("found")
    val consumeResult = consume(ctx, resultVars)

    s"""
       |if ($relationIsUnique) {
       |  // generate join key for stream side
       |  ${keyEv.code}
       |  // find matches from HashedRelation
       |  UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm
       |    .getValue(${keyEv.value});
       |  $checkCondition
       |  if (!$conditionPassed) {
       |    $matched = null;
       |    // reset the variables those are already evaluated.
       |    ${buildVars.filter(_.code == "").map(v => s"${v.isNull} = true;")
              .mkString("\n")}
       |  }
       |  $numOutput.add(1);
       |  $consumeResult
       |} else {
       |  // generate join key for stream side
       |  ${keyEv.code}
       |  // find matches from HashRelation
       |  $iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm
       |    .get(${keyEv.value});
       |  boolean $found = false;
       |  // the last iteration of this loop is to emit an empty row
       |  // if there is no matched rows.
       |  while ($matches != null && $matches.hasNext() || !$found) {
       |    UnsafeRow $matched = $matches != null && $matches.hasNext() ?
       |      (UnsafeRow) $matches.next() : null;
       |    $checkCondition
       |    if (!$conditionPassed) continue;
       |    $found = true;
       |    $numOutput.add(1);
       |    $consumeResult
       |  }
       |}
    """.stripMargin
  }

  /**
   * Generates the code for left semi join.
   */
  private def codeGenSemi(ctx: CodegenContext,
      input: Seq[ExprCode], relationTerm: String,
      relationIsUnique: String): String = {
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, _, _) = getJoinCondition(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")
    val matches = ctx.freshName("matches")
    val iteratorCls = classOf[Iterator[UnsafeRow]].getName
    val found = ctx.freshName("found")
    val consumeResult = consume(ctx, input)

    s"""
       |if ($relationIsUnique) {
       |  // generate join key for stream side
       |  ${keyEv.code}
       |  // find matches from HashedRelation
       |  UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm
       |    .getValue(${keyEv.value});
       |  if ($matched == null) continue;
       |  $checkCondition
       |  $numOutput.add(1);
       |  $consumeResult
       |} else {
       |  // generate join key for stream side
       |  ${keyEv.code}
       |  // find matches from HashRelation
       |  $iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm
       |    .get(${keyEv.value});
       |  if ($matches == null) continue;
       |  boolean $found = false;
       |  while (!$found && $matches.hasNext()) {
       |    UnsafeRow $matched = (UnsafeRow) $matches.next();
       |    $checkCondition
       |    $found = true;
       |  }
       |  if (!$found) continue;
       |  $numOutput.add(1);
       |  $consumeResult
       |}
    """.stripMargin
  }

  /**
   * Generates the code for anti join.
   */
  private def codeGenAnti(ctx: CodegenContext,
      input: Seq[ExprCode], relationTerm: String,
      relationIsUnique: String): String = {
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, antiCondition, _) =
      getJoinCondition(ctx, input, anti = true)
    val numOutput = metricTerm(ctx, "numOutputRows")
    val matches = ctx.freshName("matches")
    val iteratorCls = classOf[Iterator[UnsafeRow]].getName
    val found = ctx.freshName("found")
    val consumeResult = consume(ctx, input)

    s"""
       |if ($relationIsUnique) {
       |  // generate join key for stream side
       |  ${keyEv.code}
       |  // Check if the key has nulls.
       |  if (!($anyNull)) {
       |    // find matches from HashedRelation
       |    UnsafeRow $matched = (UnsafeRow)$relationTerm
       |      .getValue(${keyEv.value});
       |    if ($matched != null) {
       |      // Evaluate the condition.
       |      $antiCondition
       |    }
       |  }
       |  $numOutput.add(1);
       |  $consumeResult
       |} else {
       |  // generate join key for stream side
       |  ${keyEv.code}
       |  // Check if the key has nulls.
       |  if (!($anyNull)) {
       |    // find matches from HashedRelation
       |    $iteratorCls $matches = ($iteratorCls)$relationTerm
       |      .get(${keyEv.value});
       |    if ($matches != null) {
       |      // Evaluate the condition.
       |      boolean $found = false;
       |      while (!$found && $matches.hasNext()) {
       |        UnsafeRow $matched = (UnsafeRow) $matches.next();
       |        $checkCondition
       |        $found = true;
       |      }
       |      if ($found) continue;
       |    }
       |  }
       |  $numOutput.add(1);
       |  $consumeResult
       |}
    """.stripMargin
  }

  /**
   * Generates the code for existence join.
   */
  private def codeGenExistence(ctx: CodegenContext,
      input: Seq[ExprCode], relationTerm: String,
      relationIsUnique: String): String = {
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")
    val existsVar = ctx.freshName("exists")

    val matched = ctx.freshName("matched")
    val buildVars = genBuildSideVars(ctx, matched)
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars,
        expr.references)
      // filter the output via condition
      ctx.currentVars = input ++ buildVars
      val ev = BindReferences.bindReference(expr,
        streamedPlan.output ++ buildPlan.output).genCode(ctx)
      s"""
         |$eval
         |${ev.code}
         |$existsVar = !${ev.isNull} && ${ev.value};
       """.stripMargin
    } else {
      s"$existsVar = true;"
    }

    val resultVar = input ++ Seq(ExprCode("", "false", existsVar))
    val matches = ctx.freshName("matches")
    val iteratorCls = classOf[Iterator[UnsafeRow]].getName
    val consumeResult = consume(ctx, resultVar)

    s"""
       |if ($relationIsUnique) {
       |  // generate join key for stream side
       |  ${keyEv.code}
       |  // find matches from HashedRelation
       |  UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm
       |    .getValue(${keyEv.value});
       |  boolean $existsVar = false;
       |  if ($matched != null) {
       |    $checkCondition
       |  }
       |  $numOutput.add(1);
       |  $consumeResult
       |} else {
       |  // generate join key for stream side
       |  ${keyEv.code}
       |  // find matches from HashRelation
       |  $iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm
       |    .get(${keyEv.value});
       |  boolean $existsVar = false;
       |  if ($matches != null) {
       |    while (!$existsVar && $matches.hasNext()) {
       |      UnsafeRow $matched = (UnsafeRow) $matches.next();
       |      $checkCondition
       |    }
       |  }
       |  $numOutput.add(1);
       |  $consumeResult
       |}
     """.stripMargin
  }
}

object HashedRelationCache {

  type KeyType = (StructType, Seq[Expression])

  @volatile private[this] var _relationCache: Option[(Cache[
      KeyType, HashedRelation], TaskMemoryManager)] = None
  private[this] val relationCacheSize = new AtomicLong(0L)

  private[this] def initCache(): (Cache[KeyType, HashedRelation],
      TaskMemoryManager) = {
    val env = SparkEnv.get
    val cacheTimeoutSecs = Constant.DEFAULT_CACHE_TIMEOUT_SECS
    val cache = CacheBuilder.newBuilder()
        .maximumSize(50)
        .expireAfterAccess(cacheTimeoutSecs, TimeUnit.SECONDS)
        .removalListener(new RemovalListener[KeyType, HashedRelation] {
          override def onRemoval(notification: RemovalNotification[KeyType,
              HashedRelation]): Unit = {
            relationCacheSize.decrementAndGet()
            notification.getValue.close()
          }
        }).build[KeyType, HashedRelation]()
    (cache, new TaskMemoryManager(env.memoryManager, -1L))
  }

  private def getCache: (Cache[KeyType, HashedRelation], TaskMemoryManager) = {
    val c = _relationCache
    c match {
      case Some(relationCache) => relationCache
      case None => synchronized {
        _relationCache match {
          case Some(cache) => cache
          case None =>
            val relationCache = initCache()
            _relationCache = Some(relationCache)
            relationCache
        }
      }
    }
  }

  def get(schema: StructType, buildKeys: Seq[Expression], rdd: RDD[InternalRow],
      split: Partition, context: TaskContext,
      tries: Int = 1): HashedRelation = {
    try {
      val (cache, memoryManager) = getCache
      cache.get(schema -> buildKeys, new Callable[HashedRelation] {
        override def call(): HashedRelation = {
          val relation = HashedRelation(rdd.iterator(split, context),
            buildKeys, taskMemoryManager = memoryManager)
          relationCacheSize.incrementAndGet()
          relation
        }
      }).asReadOnlyCopy()
    } catch {
      case e: ExecutionException =>
        // in case of OOME from MemoryManager, try after clearing the cache
        val cause = e.getCause
        cause match {
          case _: OutOfMemoryError =>
            if (tries <= 10 && relationCacheSize.get() > 0) {
              getCache._1.invalidateAll()
              get(schema, buildKeys, rdd, split, context, tries + 1)
            } else {
              throw new RuntimeException(cause.getMessage, cause)
            }
          case _ => throw new RuntimeException(cause.getMessage, cause)
        }
      case e: Exception => throw new RuntimeException(e.getMessage, e)
    }
  }

  def remove(schema: StructType, buildKeys: Seq[Expression]): Unit = {
    getCache._1.invalidate(schema -> buildKeys)
  }

  def clear(): Unit = {
    if (relationCacheSize.get() > 0) {
      getCache._1.invalidateAll()
    }
  }

  def close(): Unit = synchronized {
    _relationCache match {
      case Some(cache) =>
        cache._1.invalidateAll()
        _relationCache = None
      case None => // nothing to be done
    }
  }
}
