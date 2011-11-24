package com.hbmr.hbase.mr;

import java.util.List;

/**
 * Sequential mapreduce job
 *
 * @author Vlad Patryshev
 */
public class SequentialJob extends BulkJob implements MrJob {
  volatile int position = 0;

  public SequentialJob(MrJob... jobs) {
    super(jobs);
  }

  SequentialJob(List<MrJob> jobs) {
    super(jobs);
  }

  public synchronized void start() {
    if (!isStarted()) {
      markStart();
      isStarted = true;

      new Thread() {
        public void run() {
          while (position < jobs.size()) {
            try {
              System.out.println(this + ": waiting, " + position + "/" + jobs.size() + " done.");
              MrJob job = jobs.get(position);
              job.waitForCompletion();
              System.out.println("===Finished: " + job.details());
              position++;
            } catch (InterruptedException e) {
              interrupt();
            }
          }
          isComplete = true;
        }
      }.start();
    }
  }

  public boolean waitForCompletion() throws InterruptedException {
    start();
    while (!isComplete) {
      sleep();
    }
    return isSuccessful();
  }

  @Override
  protected String kind() {
    return "seq";
  }

  protected void sleep() {
  }

}
