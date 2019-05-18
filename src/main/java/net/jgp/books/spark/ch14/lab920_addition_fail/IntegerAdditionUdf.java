package net.jgp.books.spark.ch14.lab920_addition_fail;

import org.apache.spark.sql.api.java.UDF2;

public class IntegerAdditionUdf implements UDF2<Integer, Integer, Integer> {

  private static final long serialVersionUID = -2162134L;

  @Override
  public Integer call(Integer t1, Integer t2) throws Exception {
    return t1 + t2;
  }

}
