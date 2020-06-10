package net.jgp.books.spark.ch14.lab911_addition_fail1

import org.apache.spark.sql.api.java.UDF2

@SerialVersionUID(-2162134L)
class IntegerAdditionScalaUdf extends UDF2[Int, Int, Int] {

  @throws[Exception]
  override def call(t1: Int, t2: Int): Int =
    t1 + t2

}

