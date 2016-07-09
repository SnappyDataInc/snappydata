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
package io.snappydata.impl

import java.sql.{Connection, Date, Timestamp, Types}
import java.util

import scala.collection.JavaConverters._

import com.pivotal.gemfirexd.snappy.ComplexTypeSerializer

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRowWithSchema, GenericRow, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * Implementation of <code>ComplexTypeSerializer</code>.
 */
final class ComplexTypeSerializerImpl(table: String, column: String,
    connection: Connection) extends ComplexTypeSerializer {

  private[this] val schema = {
    val stmt = connection.prepareCall("CALL SYS.GET_COLUMN_TABLE_SCHEMA(?, ?, ?)")
    try {
      val (schemaName, tableName) = {
        if (table.contains(".")) {
          val indexOfDot = table.indexOf(".")
          (table.substring(0, indexOfDot), table.substring(indexOfDot + 1))
        } else {
          (connection.getSchema, table)
        }
      }
      stmt.setString(1, schemaName)
      stmt.setString(2, tableName)
      stmt.registerOutParameter(3, Types.CLOB)
      stmt.execute()
      DataType.fromJson(stmt.getString(3)).asInstanceOf[StructType]
    } finally {
      stmt.close()
    }
  }

  private[this] val dataType = schema.fields.find(Utils.fieldName(_)
      .equalsIgnoreCase(column)).getOrElse(throw Utils.analysisException(
    s"Field $column does not exist in $table with schema=$schema.")).dataType

  private[this] val (isArray, struct) = dataType match {
    case a: ArrayType => (true, None)
    case m: MapType => (false, None)
    case s: StructType => (false, Some(s))
    case _ => throw Utils.analysisException(
      s"Complex type conversion: unexpected type $dataType")
  }

  private[this] lazy val serializer = CodeGeneration
      .getComplexTypeSerializer(dataType)

  private[this] lazy val bufferHolder = new BufferHolder()

  private[this] lazy val validatingConverter = ValidatingConverter(dataType,
    table, column)

  private[this] lazy val scalaConverter = Utils.createScalaConverter(dataType)

  @volatile private[this] var validated = false

  private[this] def toBytes(v: Any): Array[Byte] = {
    serializer.serialize(v, bufferHolder)
    val b = util.Arrays.copyOf(bufferHolder.buffer, bufferHolder.totalSize())
    bufferHolder.reset()
    b
  }

  override def serialize(v: Any, validateAll: Boolean): Array[Byte] = {
    // validate only once when validateAll==false
    if (v != null) {
      if (validated) {
        // resetting validated is fine because this class objects are not
        // supposed to be shared between threads (avoids try-finally)
        validated = false
        val result = toBytes(validatingConverter(v, validateAll))
        validated = true
        result
      } else {
        val result = toBytes(validatingConverter(v, validate = true))
        validated = true
        result
      }
    } else {
      toBytes(null)
    }
  }

  override def deserialize(bytes: Array[Byte], offset: Int,
      length: Int): AnyRef = struct match {
    case None =>
      if (isArray) {
        val array = new UnsafeArrayData
        array.pointTo(bytes, Platform.BYTE_ARRAY_OFFSET + offset, length)
        scalaConverter(array).asInstanceOf[Seq[_]].asJava
      } else {
        val map = new UnsafeMapData
        map.pointTo(bytes, Platform.BYTE_ARRAY_OFFSET + offset, length)
        scalaConverter(map).asInstanceOf[Map[_, _]].asJava
      }
    case Some(s) =>
      val row = new UnsafeRow
      row.pointTo(bytes, Platform.BYTE_ARRAY_OFFSET + offset, s.length, length)
      scalaConverter(row) match {
        case g: GenericRow =>
          java.util.Arrays.asList(Utils.getGenericRowValues(g): _*)
        case r: Row =>
          val length = r.length
          val list = new java.util.ArrayList[Any](length)
          var i = 0
          while (i < length) {
            list.add(r.get(i))
            i += 1
          }
          list
      }
  }
}

trait ValidatingConverter {
  def apply(v: Any, validate: Boolean): Any
}

object ValidatingConverter {

  private[impl] val objectCompatibilityMap: Map[Class[_], Seq[Class[_]]] = Map(
    StringType.getClass -> Seq(classOf[String]),
    IntegerType.getClass -> Seq(classOf[java.lang.Integer]),
    LongType.getClass -> Seq(classOf[java.lang.Long]),
    ShortType.getClass -> Seq(classOf[java.lang.Short]),
    DoubleType.getClass -> Seq(classOf[java.lang.Double]),
    FloatType.getClass -> Seq(classOf[java.lang.Float]),
    BooleanType.getClass -> Seq(classOf[java.lang.Boolean]),
    ByteType.getClass -> Seq(classOf[java.lang.Byte]),
    DateType.getClass -> Seq(classOf[Date]),
    TimestampType.getClass -> Seq(classOf[Timestamp]),
    CalendarIntervalType.getClass -> Seq(classOf[CalendarInterval]),
    BinaryType.getClass -> Seq(classOf[Array[Byte]]),
    classOf[DecimalType] -> Seq(classOf[java.math.BigDecimal],
      classOf[Decimal], classOf[BigDecimal])
  )

  def apply(dataType: DataType, table: String,
      column: String): ValidatingConverter = dataType match {
    case array: ArrayType => new ArrayValidatingConverter(array, table, column)
    case map: MapType => new MapValidatingConverter(map, table, column)
    case struct: StructType => new StructValidatingConverter(struct,
      table, column)
    case IntegerType | LongType | ShortType | DoubleType | FloatType |
         BooleanType | ByteType | CalendarIntervalType | BinaryType =>
      new IdentityValidatingConverter(dataType, table, column)
    case NullType => new NullValidatingConverter(table, column)
    case _ => new GenericValidatingConverter(dataType, table, column)
  }
}

private final class ArrayValidatingConverter(array: ArrayType,
    table: String, column: String) extends ValidatingConverter {

  private[this] val converter = ValidatingConverter(array.elementType,
    table, column + ".$0")
  private[this] val isIdentityConverter =
    converter.isInstanceOf[IdentityValidatingConverter]

  def apply(v: Any, validate: Boolean): Any = v match {
    case a: Array[Any] =>
      if (!validate && isIdentityConverter) new GenericArrayData(a)
      else new GenericArrayData(a.transform(converter(_, validate)))
    case a: Array[_] =>
      if (!validate && isIdentityConverter) new GenericArrayData(a)
      else new GenericArrayData(a.map(converter(_, validate)))
    case s: Seq[_] =>
      val a = s.toArray[Any]
      if (!validate && isIdentityConverter) new GenericArrayData(a)
      else new GenericArrayData(a.transform(converter(_, validate)))
    case c: util.Collection[_] =>
      val a = c.toArray.asInstanceOf[Array[Any]]
      if (!validate && isIdentityConverter) new GenericArrayData(a)
      else new GenericArrayData(a.transform(converter(_, validate)))
    case _: ArrayData => v
    case null => null
    case _ => throw new IllegalArgumentException(s"Cannot convert value " +
        s"of ${v.getClass} to ARRAY for $table($column). " +
        "Supported types: Object[], Collection, scala Seq")
  }
}

final class MapValidatingConverter(map: MapType,
    table: String, column: String) extends ValidatingConverter {

  private[this] val keyConverter = ValidatingConverter(map.keyType,
    table, column + ".$1")
  private[this] val valueConverter = ValidatingConverter(map.valueType,
    table, column + ".$2")
  private[this] val allIdentityConverters =
    keyConverter.isInstanceOf[IdentityValidatingConverter] &&
        valueConverter.isInstanceOf[IdentityValidatingConverter]

  def apply(v: Any, validate: Boolean): Any = {
    val mapValues = v match {
      case m: scala.collection.Map[_, _] => m
      case m: util.Map[_, _] => m.asScala
      case _: MapData => return v
      case null => return null
      case _ => throw new IllegalArgumentException(s"Cannot convert value " +
          s"of ${v.getClass} to MAP for $table($column). " +
          "Supported types: Map, scala Map")
    }
    val len = mapValues.size
    val keys = new Array[Any](len)
    val values = new Array[Any](len)
    var i = 0
    val itr = mapValues.iterator
    if (!validate && allIdentityConverters) {
      while (i < len) {
        val e = itr.next()
        keys(i) = e._1
        values(i) = e._2
        i += 1
      }
    } else {
      while (i < len) {
        val e = itr.next()
        keys(i) = keyConverter(e._1, validate)
        values(i) = valueConverter(e._2, validate)
        i += 1
      }
    }
    ArrayBasedMapData(keys, values)
  }
}

final class StructValidatingConverter(struct: StructType,
    table: String, column: String) extends ValidatingConverter {

  private[this] var allIdentityConverters = true
  private[this] val converters = struct.map { f =>
    val converter = ValidatingConverter(f.dataType, table,
      s"$column.${Utils.fieldName(f)}")
    if (allIdentityConverters &&
        !converter.isInstanceOf[IdentityValidatingConverter]) {
      allIdentityConverters = false
    }
    converter
  }

  def apply(v: Any, validate: Boolean): Any = {
    val values: Array[Any] = v match {
      case a: Array[Any] => a
      case a: Array[_] => a.toSeq.toArray
      case s: Seq[_] => s.toArray
      case c: util.Collection[_] => c.toArray.asInstanceOf[Array[Any]]
      case r: GenericRow => Utils.getGenericRowValues(r)
      case r: Row =>
        val len = r.length
        val arr = new Array[Any](len)
        var i = 0
        while (i < len) {
          arr(i) = r.get(i)
          i += 1
        }
        arr
      case p: Product =>
        val len = p.productArity
        val arr = new Array[Any](len)
        val itr = p.productIterator
        var i = 0
        while (i < len) {
          arr(i) = itr.next()
          i += 1
        }
        arr
      case r: InternalRow => checkStruct(r.numFields, struct); return r
      case null => null
      case _ => throw new IllegalArgumentException(s"Cannot convert value " +
          s"of ${v.getClass} to STRUCT for $table($column). " +
          "Supported types: Object[], Collection, scala Seq, " +
          "scala Product, spark Row")
    }
    val len = values.length
    checkStruct(len, struct)
    // check and transform each of the fields too
    if (validate || !allIdentityConverters) {
      var i = 0
      val itr = converters.iterator
      while (i < len) {
        val converter = itr.next()
        values(i) = converter(values(i), validate)
        i += 1
      }
    }
    new GenericInternalRowWithSchema(values, struct)
  }

  private[this] def checkStruct(len: Int, dataType: StructType) = {
    if (len != dataType.length) {
      throw new IllegalArgumentException("Incompatible value collection with" +
          s" $len fields for $table($column). Expected schema=$dataType")
    }
  }
}

final class IdentityValidatingConverter(dataType: DataType,
    table: String, column: String) extends ValidatingConverter {

  private[this] val expectedClass = ValidatingConverter.objectCompatibilityMap(
    dataType.getClass).head

  def apply(v: Any, validate: Boolean): Any = {
    if (!validate || v == null || v.getClass == expectedClass) v
    else {
      throw new IllegalArgumentException("Cannot convert value of " +
          s"${v.getClass} to ${Utils.toUpperCase(dataType.typeName)} for " +
          s"$table($column). Supported type: ${expectedClass.getSimpleName}")
    }
  }
}

final class NullValidatingConverter(table: String,
    column: String) extends ValidatingConverter {

  def apply(v: Any, validate: Boolean): Any = {
    if (!validate || v == null) v
    else {
      throw new IllegalArgumentException("Cannot convert value of " +
          s"${v.getClass} to NULL for $table($column). Value should be null")
    }
  }
}

final class GenericValidatingConverter(dataType: DataType,
    table: String, column: String) extends ValidatingConverter {

  private[this] val converter = Utils.createCatalystConverter(dataType)

  def apply(v: Any, validate: Boolean): Any = {
    // converter can result in a match error, so change it to proper exception
    try {
      converter(v)
    } catch {
      case _: MatchError =>
        throw new IllegalArgumentException("Cannot convert value of " +
            s"${v.getClass} to ${Utils.toUpperCase(dataType.typeName)} for " +
            s"$table($column). Supported types: ${ValidatingConverter
            .objectCompatibilityMap(dataType.getClass).map(_.getName)
            .mkString(", ")}")
    }
  }
}
