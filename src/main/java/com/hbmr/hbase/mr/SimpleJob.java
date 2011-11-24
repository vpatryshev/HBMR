package com.hbmr.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

/**
 * Simple mapreduce job, just a delegate to hadoop mr job
 *
 * @author Vlad Patryshev
 */
public class SimpleJob extends CommonJob {
//  private static final float SETUP_WEIGHT = 0.2f;
  private static final float MAP_WEIGHT = 0.5f;
  private static final float REDUCE_WEIGHT = 0.5f;

  private Job job;

  SimpleJob(Job job) {
    this.job = job;
  }

  public synchronized void start() {
    try {
      if (!isStarted()) {
        markStart();
        job.submit();
        isStarted = true;
      }
    } catch (Exception e) {
      log.error("Could not start " + this, e);
    }
  }

  public boolean isComplete() {
    if (isStarted && !isComplete) {
      try {
        isComplete = job.isComplete();
      } catch (IOException e) {
        log.error("Could not check state " + this, e);
        isSuccessful = false;
        return true;
      } finally {
        markStop();
      }
    }
    return isComplete;
  }

  public boolean isSuccessful() {
    if (!isStarted || !isComplete() || !isSuccessful) return false;

    try {
      isSuccessful = job.isSuccessful();
    } catch (IOException e) {
      log.error("Could not check state " + this, e);
      isSuccessful = false;
    }
    return isSuccessful;
  }

  public boolean waitForCompletion() throws InterruptedException {
    if (isComplete()) return true;
    System.out.println(details() + ": waiting");

    try {
      start();
      isSuccessful = job.waitForCompletion(VERBOSITY);
      isComplete = true;
    } catch (IOException e) {
      log.error("Could not wait " + this, e);
      isSuccessful = false;
    } catch (ClassNotFoundException e) {
      log.error("Could not wait " + this, e);
      isSuccessful = false;
    }
    return isSuccessful();
  }

  public float progress() {
    try {
      lastProgress = !isStarted ? 0.0f :
          MAP_WEIGHT * job.mapProgress() + REDUCE_WEIGHT * job.reduceProgress();
    } catch (IOException e) {
      log.error("Could not get progress " + this, e);
    }
    return lastProgress;
  }

  public String getName() {
    return job.getJobName();
  }

  public String details() {
    String head = String.format("\"%s\" time: %d ", job.getJobName(), timeRunning());
    try {
      return isComplete() ? head +
          String.format("Complete, success: %b", isSuccessful()) :
          isStarted() ? head +
              String.format("\"%s\"\ttime: %d, completed: %b, success: %b, progress: map: %4.2f, reduce: %4.2f",
                  job.getJobName(), timeRunning(), isComplete(), isSuccessful(),
                  job.mapProgress(),
                  job.reduceProgress()) :
              String.format("Name: %s, NOT STARTED YET", job.getJobName());
    } catch (IOException e) {
      return head + ": exception " + e.getMessage();
    }
  }

  public String explainError() {
    return isComplete() && !isSuccessful() ? (getName() + " failed") : "";
  }

}
