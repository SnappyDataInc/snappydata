/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.ColumnDeltaEncoder
import org.apache.spark.sql.execution.columnar.impl.ColumnDelta
import org.apache.spark.sql.execution.{SparkPlan, TableExec}
import org.apache.spark.sql.sources.{ConnectionProperties, DestroyRelation}
import org.apache.spark.sql.types.StructType

/**
 * Generated code plan for updates into a column table.
 */
case class ColumnUpdateExec(child: SparkPlan, resolvedName: String,
    partitionColumns: Seq[String], partitionExpressions: Seq[Expression], numBuckets: Int,
    tableSchema: StructType, externalStore: ExternalStore, relation: Option[DestroyRelation],
    updateColumns: Seq[Attribute], updateExpressions: Seq[Expression],
    keyColumns: Seq[Attribute], connProps: ConnectionProperties, onExecutor: Boolean)
    extends TableExec(partitionColumns, tableSchema, relation, onExecutor) {

  assert(updateColumns.length == updateExpressions.length)

  /**
   * The indexes below are the final ones that go into ColumnFormatKey(columnIndex).
   * For deltas the convention is to use negative values beyond those available for
   * each hierarchy depth. So starting at DELTA_STATROW index of -2, the first column
   * will use indexes -3, -4, -5 for hierarchy depth 3, second column will use
   * indexes -6, -7, -8 and so on. The values below are initialized to the first value
   * in the series.
   */
  private val updateIndexes = updateColumns.map(a => ColumnDelta.deltaColumnIndex(
    Utils.fieldIndex(tableSchema.toAttributes, a.name,
      sqlContext.conf.caseSensitiveAnalysis), hierarchyDepth = 0))

  override protected def opType: String = "Update"

  override def nodeName: String = "ColumnUpdate"

  @transient private var batchIdTerm: String = _
  @transient private var batchOrdinal: String = _
  @transient private var finishUpdate: String = _
  @transient private var updateMetric: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val result = ctx.freshName("result")
    val childProduce = doChildProduce(ctx)
    s"""
       |$childProduce
       |if ($batchOrdinal > 0) {
       |  $finishUpdate($batchIdTerm);
       |}
       |final long $result = ${if (updateMetric eq null) "0L" else metricValue(updateMetric)};
       |${consume(ctx, Seq(ExprCode("", "false", result)))}
    """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    // use an array of delta encoders and cursors
    val numColumns = updateColumns.length
    val deltaEncoders = ctx.freshName("deltaEncoders")
    val cursors = ctx.freshName("cursors")
    val index = ctx.freshName("index")
    batchOrdinal = ctx.freshName("batchOrdinal")
    val lastColumnBatchId = ctx.freshName("lastColumnBatchId")
    val updateSchema = StructType.fromAttributes(updateColumns)
    finishUpdate = ctx.freshName("finishUpdate")
    val initializeEncoders = ctx.freshName("initializeEncoders")
    val schemaTerm = ctx.addReferenceObj("updateSchema", updateSchema,
      classOf[StructType].getName)
    val deltaIndexes = ctx.addReferenceObj("deltaIndexes", updateIndexes)
    val externalStoreTerm = ctx.addReferenceObj("externalStore", externalStore)
    val tableName = ctx.addReferenceObj("columnTable", resolvedName, "java.lang.String")
    val partitionId = ctx.freshName("partitionId")
    updateMetric = if (onExecutor) null else metricTerm(ctx, "numUpdateRows")

    val deltaEncoderClass = classOf[ColumnDeltaEncoder].getName
    val columnBatchClass = classOf[ColumnBatch].getName

    ctx.addMutableState(s"$deltaEncoderClass[]", deltaEncoders, "")
    ctx.addMutableState("long[]", cursors, "")
    ctx.addMutableState("UTF8String", lastColumnBatchId, "")
    ctx.addMutableState("int", batchOrdinal, "")
    ctx.addMutableState("int", partitionId, "")
    ctx.addPartitionInitializationStatement(s"$partitionId = partitionIndex;")

    ctx.INPUT_ROW = null
    ctx.currentVars = input
    // bind the update expressions followed by key columns
    val allExpressions = updateExpressions ++ keyColumns
    val updateInput = ctx.generateExpressions(allExpressions.map(
      u => ExpressionCanonicalizer.execute(BindReferences.bindReference(
        u, child.output))), doSubexpressionElimination = true)
    ctx.currentVars = null

    // first column in keyColumns should be column batchId
    assert(keyColumns.head.name.equalsIgnoreCase(ColumnDelta.COLUMN_BATCH_ID_COLUMN))
    // second column in keyColumns should be ordinal in the column
    assert(keyColumns(1).name.equalsIgnoreCase(ColumnDelta.COLUMN_BATCH_ORDINAL_COLUMN))

    val numKeys = keyColumns.length
    batchIdTerm = updateInput(updateInput.length - numKeys).value
    val positionTerm = updateInput(updateInput.length - numKeys + 1).value

    ctx.addNewFunction(initializeEncoders,
      s"""
         |private void $initializeEncoders() {
         |  $deltaEncoders = new $deltaEncoderClass[$numColumns];
         |  $cursors = new long[$numColumns];
         |  for (int $index = 0; $index < $numColumns; $index++) {
         |    $deltaEncoders[$index] = new $deltaEncoderClass(0);
         |    $cursors[$index] = $deltaEncoders[$index].initialize(
         |        $schemaTerm.fields()[$index], $deltaEncoderClass.INIT_SIZE());
         |  }
         |}
      """.stripMargin)
    // Creating separate encoder write functions instead of inlining for wide-schemas
    // in updates (especially with support for putInto being added). Performance should
    // be about the same since JVM will inline if the number of columns is small.
    val callEncoders = updateColumns.zipWithIndex.map { case (col, i) =>
      val function = ctx.freshName("encoderFunction")
      val ordinal = ctx.freshName("ordinal")
      val dataType = col.dataType
      val encoderTerm = s"$deltaEncoders[$i]"
      val cursorTerm = s"$cursors[$i]"
      val ev = updateInput(i)
      ctx.addNewFunction(function,
        s"""
           |private void $function(int $ordinal, int columnPosition,
           |    ${ctx.javaType(dataType)} ${ev.value}, boolean ${ev.isNull}) {
           |  $encoderTerm.setUpdatePosition(columnPosition);
           |  ${ColumnWriter.genCodeColumnWrite(ctx, dataType, col.nullable, encoderTerm,
                cursorTerm, ev, ordinal)}
           |}
        """.stripMargin)
      // code for invoking the function
      s"$function($batchOrdinal, (int)$positionTerm, ${ev.value}, ${ev.isNull});"
    }.mkString("\n")
    ctx.addNewFunction(finishUpdate,
      s"""
         |private void $finishUpdate(UTF8String $batchIdTerm) {
         |  if (!$batchIdTerm.equals($lastColumnBatchId)) {
         |    if ($lastColumnBatchId == null) return;
         |    // finish previous encoders, put into table and re-initialize
         |    final java.nio.ByteBuffer[] buffers = new java.nio.ByteBuffer[$numColumns];
         |    for (int $index = 0; $index < $numColumns; $index++) {
         |      buffers[$index] = $deltaEncoders[$index].finish($cursors[$index]);
         |    }
         |    // TODO: SW: delta stats row (can have full limits for those columns)
         |    final $columnBatchClass columnBatch = $columnBatchClass.apply(
         |        $batchOrdinal, buffers, new byte[0], $deltaIndexes);
         |    // maxDeltaRows is -1 so that insert into row buffer is never considered
         |    $externalStoreTerm.storeColumnBatch($tableName, columnBatch,
         |        $partitionId, $lastColumnBatchId, -1);
         |    ${if (updateMetric eq null) "" else s"$updateMetric.${metricAdd(batchOrdinal)};"}
         |    $initializeEncoders();
         |    $lastColumnBatchId = $batchIdTerm;
         |    $batchOrdinal = 0;
         |  }
         |}
      """.stripMargin)

    s"""
       |${evaluateVariables(updateInput)}
       |// finish and apply update if the next column batch ID is seen
       |$finishUpdate($batchIdTerm);
       |// write to the encoders
       |$callEncoders
       |$batchOrdinal++;
    """.stripMargin
  }
}
