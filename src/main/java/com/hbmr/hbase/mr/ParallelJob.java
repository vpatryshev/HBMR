package com.hbmr.hbase.mr;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * Parallel mapreduce job
 *
 * @author Vlad Patryshev
 */
public class ParallelJob extends BulkJob implements MrJob {

// TODO(vlad): pass conf and extract conf
  public final static int FORK_FACTOR = 3;
  private int forkFactor = FORK_FACTOR;
  private List<MrJob> batch;

  ParallelJob(MrJob... jobs) {
    super(jobs);
  }

  public ParallelJob(Collection<MrJob> jobs) {
    super(jobs);
  }

  public synchronized void start() {
    if (!isStarted()) {
      markStart();
      isStarted = true;
      for (MrJob job : jobs) {
        job.start();
      }
    }
  }

  public boolean waitForCompletion() throws InterruptedException {
    System.out.println(this + ": waiting now");
    start();
    boolean waiting = true;
    while (waiting) {
      waiting = false;
      for (MrJob j : jobs) {
        if (!j.isStarted()) j.start();
        if (!j.isComplete()) {
          waiting = true;
          System.out.println("waiting for \"" + j.getName() + "\"");
        }
      }
      sleep();
    }
    isComplete = true;
    return isSuccessful();
  }

  @Override
  protected String kind() {
    return "par";
  }

}
