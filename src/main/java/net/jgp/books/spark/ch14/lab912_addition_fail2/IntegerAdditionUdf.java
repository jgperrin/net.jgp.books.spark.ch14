package net.jgp.books.spark.ch14.lab912_addition_fail2;

import org.apache.spark.sql.api.java.UDF2;

/**
 * Return type to String
 * 
 * @author jgp
 */
public class IntegerAdditionUdf implements UDF2<Integer, Integer, String> {

  private static final long serialVersionUID = -2162134L;

  @Override
  public String call(Integer t1, Integer t2) throws Exception {
    return Integer.toString(t1 + t2);
  }

}
