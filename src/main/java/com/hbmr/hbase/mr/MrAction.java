package com.hbmr.hbase.mr;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

/**
 * A simplified interface for mapreduce
 *
 * @author Vlad Patryshev
 */
public abstract class MrAction {

  /**
   * Default, empty, implementation of map
   *
   * @param rowkey
   * @param inputData
   * @param config
   * @return the key-value map
   */
  public Map<String, String> map(String rowkey, Result inputData, Configuration config) {
    return Collections.emptyMap();
  }

  /**
   * TODO: explain
   *
   * @param rowkey
   * @param inputData
   * @param context
   * @return
   * @throws InterruptedException
   * @throws IOException
   */
  Map<String, String> map(String rowkey, Result inputData, Mapper<ImmutableBytesWritable, Result, Writable, Writable>.Context context) throws IOException, InterruptedException {
    return map(rowkey, inputData, context.getConfiguration());
  }

  /**
   * TODO: explain
   *
   * @param valuesToReduce
   * @param config
   * @return
   */
  public String reduce(Iterable<String> valuesToReduce, Configuration config) {
    return null;
  }
}
