package net.jgp.books.spark.ch14.lab910_addition

import org.apache.spark.sql.api.java.UDF2

@SerialVersionUID(-2162134L)
class StringAdditionScalaUdf extends UDF2[String, String, String] {

  @throws[Exception]
  override def call(t1: String, t2: String): String =
    t1 + t2

}
