package com.hbmr.hbase.mr.samples;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.hbmr.hbase.mr.RowAction;

/**
 * Sample mapreduce action: counts rows in a table.
 *
 * @author Vlad Patryshev
 */
public abstract class Update extends RowAction {

  @Override
  public void process(String rowkey, Result inputData, Mapper<ImmutableBytesWritable, Result, Writable, Writable>.Context context) throws IOException, InterruptedException {
    for (Row row : transform(rowkey, inputData, context.getConfiguration())) {
      context.write(null, row);
    }
  }

  /**
   * Transforms a row into output sequence (can be Deletes or Puts)
   *
   *
   * @param rowkey current row key
   * @param inputData the value at that key
   * @param config configuration
   * @return a list of rows for output
   */
  public abstract List<? extends Row> transform(String rowkey, Result inputData, Configuration config);
}
