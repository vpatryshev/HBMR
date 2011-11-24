package com.hbmr.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

/**
 * A version of hadoop mapreduce output committer that does nothing - good for hbase.
 *
 * @author Vlad Patryshev
 */
public class HBaseCommitter extends OutputCommitter {

  @Override
  public void setupJob(org.apache.hadoop.mapred.JobContext jobContext) throws IOException {}

  @Override
  public void cleanupJob(JobContext jobContext) throws IOException {}

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {}

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {}

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {}

}