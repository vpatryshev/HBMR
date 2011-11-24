package com.hbmr.hbase.mr;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Writable;

public abstract class RowAction extends MrAction {

  public abstract void process(String rowkey, Result inputData, Mapper<ImmutableBytesWritable, Result, Writable, Writable>.Context context) throws IOException, InterruptedException;

  @Override public Map<String, String> map(String rowkey, Result inputData, Mapper<ImmutableBytesWritable, Result, Writable, Writable>.Context context) throws IOException, InterruptedException {
    process(rowkey, inputData, context);
    return Collections.emptyMap();
  }

  public String reduce(Iterable<String> valuesToReduce, Configuration config) {
    return null;
  }

}
