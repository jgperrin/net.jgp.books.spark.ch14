package net.jgp.books.spark.ch14.lab900_in_range

import org.apache.spark.sql.api.java.UDF2

/**
 * The UDF code itself provides the plumbing between the service code and
 * the application code.
 *
 * @author rambabu.posa
 *
 */
@SerialVersionUID(-21621751L)
class InRangeScalaUdf extends UDF2[String, Integer, Boolean] {

  def call(range: String, event: Integer): Boolean =
    InRangeScalaService.call(range, event)
}
