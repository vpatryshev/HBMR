package com.hbmr.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * An hbase-specific version of hadoop mapreduce output format
 *
 * @author Vlad Patryshev
 */
public class HBaseOutputFormat extends TableOutputFormat<ImmutableBytesWritable> {
  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    super.setConf(conf);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    new HTable(conf, conf.get(OUTPUT_TABLE));
  }
}