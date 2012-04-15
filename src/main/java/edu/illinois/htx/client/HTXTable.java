package edu.illinois.htx.client;

import java.io.Closeable;
import java.io.IOException;


public interface HTXTable extends Closeable {

  public Result get(Transaction ta, Get get) throws IOException;

  public void put(Transaction ta, Put put) throws IOException;

  public void delete(Transaction ta, Delete delete) throws IOException;

}
