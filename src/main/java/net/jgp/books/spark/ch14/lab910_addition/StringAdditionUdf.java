package net.jgp.books.spark.ch14.lab910_addition;

import org.apache.spark.sql.api.java.UDF2;

public class StringAdditionUdf implements UDF2<String, String, String> {

  private static final long serialVersionUID = -2162134L;

  @Override
  public String call(String t1, String t2) throws Exception {
    return t1 + t2;
  }

}
