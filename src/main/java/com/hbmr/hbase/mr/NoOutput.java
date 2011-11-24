package com.hbmr.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class NoOutput extends OutputFormat<ImmutableBytesWritable, Writable> {

  @Override
  public RecordWriter<ImmutableBytesWritable, Writable> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return null;
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException,
      InterruptedException {
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new TableOutputCommitter();
  }

}
