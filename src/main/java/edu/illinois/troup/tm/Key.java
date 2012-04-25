package edu.illinois.troup.tm;

public interface Key {

  /**
   * must override equals contract
   * 
   * @param obj
   * @return
   */
  @Override
  public boolean equals(Object obj);

  /**
   * must override hash code
   * 
   * @return
   */
  @Override
  public int hashCode();

}