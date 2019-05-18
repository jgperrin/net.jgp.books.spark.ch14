package net.jgp.books.spark.ch14.lab911_addition_fail1;

import org.apache.spark.sql.api.java.UDF2;

public class IntegerAdditionUdf implements UDF2<Integer, Integer, Integer> {

  private static final long serialVersionUID = -2162134L;

  @Override
  public Integer call(Integer t1, Integer t2) throws Exception {
    return t1 + t2;
  }

}
