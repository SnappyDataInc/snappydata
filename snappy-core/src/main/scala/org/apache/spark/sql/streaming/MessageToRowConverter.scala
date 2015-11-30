package org.apache.spark.sql.streaming

import org.apache.spark.sql.catalyst.InternalRow

/**
 * Created by ymahajan on 4/11/15.
 */
trait MessageToRowConverter extends Serializable {
  def toRow(message: Any): InternalRow

  def getTargetType(): scala.Predef.Class[_]
}


