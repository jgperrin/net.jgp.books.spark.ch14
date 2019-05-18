package net.jgp.books.spark.ch14.lab911_addition_fail1;

import org.apache.spark.sql.api.java.UDF2;

public class StringAdditionUdf implements UDF2<String, String, String> {

  private static final long serialVersionUID = -2162134L;

  @Override
  public String call(String t1, String t2) throws Exception {
    return t1 + t2;
  }

}
