package com.hbmr.hbase.mr;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * A collection of mapreduce jobs. Can be parallel or sequential or you name it (a priority queue? have to think about it)
 *
 * @author Vlad Patryshev
 */
public abstract class BulkJob extends CommonJob {

  protected List<MrJob> jobs;

  BulkJob(MrJob... jobs) {
    this.jobs = Lists.newArrayList(jobs);
//      Configuration conf = JobClient.this.getConf();
//      this.completionPollIntervalMillis = conf.getInt(COMPLETION_POLL_INTERVAL_KEY,
//          DEFAULT_COMPLETION_POLL_INTERVAL);
//      if (this.completionPollIntervalMillis < 1) {
//        LOG.warn(COMPLETION_POLL_INTERVAL_KEY + " has been set to an invalid value; "
//            + "replacing with " + DEFAULT_COMPLETION_POLL_INTERVAL);
//        this.completionPollIntervalMillis = DEFAULT_COMPLETION_POLL_INTERVAL;
//      }
  }

  BulkJob(Collection<MrJob> jobs) {
    this.jobs = Lists.newArrayList(jobs);
  }

  public void add(MrJob job) {
    if (isStarted()) throw new IllegalStateException("too late to add jobs to a running group");
    jobs.add(job);
  }

  protected abstract String kind();

  public float progress() {
    float p = 0.0f;
    for (MrJob job : jobs) {
      p += job.progress() / jobs.size();
    }
    return p;
  }

  public boolean isComplete() {
    if (!isComplete) {
      for (MrJob job : jobs) {
        if (!job.isComplete()) return false;
      }
      isComplete = true;
    }
    return isComplete;
  }

  public boolean isSuccessful() {
    boolean is = true;
    for (MrJob job : jobs) {
      if (!job.isSuccessful()) is = false;
    }
    isSuccessful = is;
    return super.isSuccessful();
  }

  public String getName() {
    StringBuilder sb = new StringBuilder();
    for (MrJob job : jobs) {
      if (sb.length() > 0) sb.append(", ");
      sb.append(job.getName());
    }
    return kind() + "(" + sb + ")";
  }

  public String details() {
    StringBuilder sb = new StringBuilder();
    for (MrJob job : jobs) {
      if (sb.length() > 0) sb.append(", ");
      sb.append(job.details());
    }
    return kind() + "(" + sb + "): " + fullStatus();
  }

  public String explainError() {
    StringBuilder sb = new StringBuilder();
    for (MrJob job : jobs) {
      String explanation = job.explainError();
      if (sb.length() > 0) sb.append(", ");
      if (!explanation.isEmpty()) sb.append(job.details());
    }
    return sb.length() == 0 ? "" : (kind() + "(" + sb + "): " + status());
  }
}
