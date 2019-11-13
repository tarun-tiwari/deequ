package com.amazon.deequ.analyzers

import com.amazon.deequ.analyzers.Analyzers._
import com.amazon.deequ.analyzers.Preconditions.{hasColumn, isString}
import org.apache.spark.sql.functions.{avg, count, length, sum}
import org.apache.spark.sql.types.{DoubleType, LongType, StructType}
import org.apache.spark.sql.{Column, Row}

case class MeanLength(column: String, where: Option[String] = None)
  extends StandardScanShareableAnalyzer[MeanState]("MeanLength", column) {

  override def aggregationFunctions(): Seq[Column] = {
    //    avg(length(conditionalSelection(column, where))).cast(DoubleType) :: Nil
    //min(length(conditionalSelection(column, where))).cast(DoubleType) :: Nil

    sum(length(conditionalSelection(column, where))).cast(DoubleType) ::
      count(length(conditionalSelection(column, where))).cast(LongType) :: Nil
  }

  override def fromAggregationResult(result: Row, offset: Int): Option[MeanState] = {
    ifNoNullsIn(result, offset, howMany = 2) { _ =>
      MeanState(result.getDouble(offset), result.getLong(offset + 1)): MeanState
    }
  }

  override protected def additionalPreconditions(): Seq[StructType => Unit] = {
    hasColumn(column) :: isString(column) :: Nil
  }
}
