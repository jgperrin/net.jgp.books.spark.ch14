package net.jgp.books.spark.ch14.lab920_column_as_parameter

import org.apache.spark.sql.api.java.UDF1
import scala.collection.Seq

@SerialVersionUID(8331L)
class ColumnAdditionScalaUdf extends UDF1[Seq[Int], Int] {

  @throws[Exception]
  override def call(t1: Seq[Int]): Int = {
    var res = 0
    for (value <- t1) {
      res += value
    }
    res
  }

}
