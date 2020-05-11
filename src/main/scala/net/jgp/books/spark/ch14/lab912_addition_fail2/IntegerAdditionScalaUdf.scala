package net.jgp.books.spark.ch14.lab912_addition_fail2

import org.apache.spark.sql.api.java.UDF2

/**
 * Return type to String
 *
 * @author rambabu.posa
 */
@SerialVersionUID(-2162134L)
class IntegerAdditionScalaUdf extends UDF2[Int, Int, String] {

  @throws[Exception]
  override def call(t1: Int, t2: Int): String =
    (t1 + t2).toString

}
