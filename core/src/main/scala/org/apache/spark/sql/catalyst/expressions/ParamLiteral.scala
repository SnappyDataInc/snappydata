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

package org.apache.spark.sql.catalyst.expressions

import java.util.Objects

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

case class ParamLiteral(l: Literal, pos: Int) extends LeafExpression {

  override def hashCode(): Int = {
    31 * (31 * Objects.hashCode(dataType)) + Objects.hashCode(pos)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case pl: ParamLiteral =>
        pl.l.dataType == l.dataType && pl.pos == pos
      case _ => false
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // change the isNull and primitive to consts, to inline them
    val value = l.value
    dataType match {
      case BooleanType =>
        val isNull = ctx.freshName("isNull")
        assert(value.isInstanceOf[Boolean], s"unexpected type $dataType instead of BooleanType")
        val valueRef = ctx.addReferenceObj("literal",
          LiteralValue(value, pos))
        val valueTerm = ctx.freshName("value")
        ev.copy(
          s"""
             |final boolean $isNull = $valueRef.value() == null;
             |final boolean $valueTerm = $isNull ? ${ctx.defaultValue(dataType)}
             |    : ((Boolean)$valueRef.value()).booleanValue();
           """.stripMargin, isNull, valueTerm)
      case FloatType =>
        val isNull = ctx.freshName("isNull")
        assert(value.isInstanceOf[Float], s"unexpected type $dataType instead of FloatType")
        val valueRef = ctx.addReferenceObj("literal",
          LiteralValue(value, pos))
        val valueTerm = ctx.freshName("value")
        ev.copy(
          s"""
             |final boolean $isNull = $valueRef.value() == null;
             |final float $valueTerm = $isNull ? ${ctx.defaultValue(dataType)}
             |    : ((Float)$valueRef.value()).floatValue();
           """.stripMargin, isNull, valueTerm)
      case DoubleType =>
        val isNull = ctx.freshName("isNull")
        assert(value.isInstanceOf[Double], s"unexpected type $dataType instead of DoubleType")
        val valueRef = ctx.addReferenceObj("literal",
          LiteralValue(value, pos))
        val valueTerm = ctx.freshName("value")
        ev.copy(
          s"""
             |final boolean $isNull = $valueRef.value() == null;
             |final double $valueTerm = $isNull ? ${ctx.defaultValue(dataType)}
             |    : ((Double)$valueRef.value()).doubleValue();
           """.stripMargin, isNull, valueTerm)
      case ByteType =>
        val isNull = ctx.freshName("isNull")
        assert(value.isInstanceOf[Byte], s"unexpected type $dataType instead of ByteType")
        val valueRef = ctx.addReferenceObj("literal",
          LiteralValue(value, pos))
        val valueTerm = ctx.freshName("value")
        ev.copy(
          s"""
             |final boolean $isNull = $valueRef.value() == null;
             |final byte $valueTerm = $isNull ? ${ctx.defaultValue(dataType)}
             |    : ((Byte)$valueRef.value()).byteValue();
           """.stripMargin, isNull, valueTerm)
      case ShortType =>
        val isNull = ctx.freshName("isNull")
        assert(value.isInstanceOf[Short], s"unexpected type $dataType instead of ShortType")
        val valueRef = ctx.addReferenceObj("literal",
          LiteralValue(value, pos))
        val valueTerm = ctx.freshName("value")
        ev.copy(
          s"""
             |final boolean $isNull = $valueRef.value() == null;
             |final short $valueTerm = $isNull ? ${ctx.defaultValue(dataType)}
             |    : ((Short)$valueRef.value()).shortValue();
           """.stripMargin, isNull, valueTerm)
      case IntegerType | DateType =>
        val isNull = ctx.freshName("isNull")
        assert(value.isInstanceOf[Int], s"unexpected type $dataType instead of DateType or IntegerType")
        val valueRef = ctx.addReferenceObj("literal",
          LiteralValue(value, pos))
        val valueTerm = ctx.freshName("value")
        ev.copy(
          s"""
             |final boolean $isNull = $valueRef.value() == null;
             |final int $valueTerm = $isNull ? ${ctx.defaultValue(dataType)}
             |    : ((Integer)$valueRef.value()).intValue();
           """.stripMargin, isNull, valueTerm)
      case TimestampType | LongType =>
        val isNull = ctx.freshName("isNull")
        assert(value.isInstanceOf[Long], s"unexpected type $dataType instead of TimestampType or LongType")
        val valueRef = ctx.addReferenceObj("literal",
          LiteralValue(value, pos))
        val valueTerm = ctx.freshName("value")
        ev.copy(
          s"""
             |final boolean $isNull = $valueRef.value() == null;
             |final long $valueTerm = $isNull ? ${ctx.defaultValue(dataType)}
             |    : ((Long)$valueRef.value()).longValue();
           """.stripMargin, isNull, valueTerm)
      case NullType =>
        val valueTerm = ctx.freshName("value")
        ev.copy(s"final Object $valueTerm = null")
      case other =>
        val valueRef = ctx.addReferenceObj("literal",
          LiteralValue(value, pos))
        val isNull = ctx.freshName("isNull")
        val valueTerm = ctx.freshName("value")
        val objectTerm = ctx.freshName("obj")
        ev.copy(code =
          s"""
          Object $objectTerm = $valueRef.value();
          final boolean $isNull = $objectTerm == null;
          ${ctx.javaType(this.dataType)} $valueTerm = $objectTerm != null
             ? (${ctx.boxedType(this.dataType)})$objectTerm : null;
          """, isNull, valueTerm)
    }
  }

  override def nullable: Boolean = l.nullable

  override def eval(input: InternalRow): Any = l.eval()

  override def dataType: DataType = l.dataType
}

case class LiteralValue(var value: Any, var position: Int)
  extends KryoSerializable {

  override def write(kryo: Kryo, output: Output): Unit = {
    kryo.writeClassAndObject(output, value)
    output.writeVarInt(position, true)
  }

  override def read(kryo: Kryo, input: Input): Unit = {
    value = kryo.readClassAndObject(input)
    position = input.readVarInt(true)
  }
}
