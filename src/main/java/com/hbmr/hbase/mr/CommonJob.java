package com.hbmr.hbase.mr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hbmr.common.Clock;
import com.hbmr.common.RealClock;

/**
 * Common functionality for mapreduce job
 *
 * @author Vlad Patryshev
 */
public abstract class CommonJob implements MrJob {
  static final Logger log = LoggerFactory.getLogger(MrJob.class);
  static final boolean VERBOSITY = false;
  protected Float lastProgress = 0.0f; // for atomicity, use capital 'F' - vlad
  protected volatile boolean isStarted = false;
  protected volatile boolean isComplete = false;
  protected volatile boolean isSuccessful = true;
  protected volatile long startTime;
  protected volatile long stopTime = -1;
  protected Clock clock = new RealClock();
  public static final int DEFAULT_COMPLETION_POLL_INTERVAL = 5000;
  private long completionPollIntervalMillis = DEFAULT_COMPLETION_POLL_INTERVAL;

  public boolean isStarted() {
    return isStarted;
  }

  public boolean isComplete() {
    markStop();
    return isComplete;
  }

  public boolean isSuccessful() {
    return isStarted() && isComplete() && isSuccessful;
  }

  protected void markStart() {
    startTime = clock.now();
    log.info("Starting job " + getName());
  }

  protected void markStop() {
    if (stopTime < 0 && isComplete) {
      stopTime = clock.now();
      log.info("Stopped job " + getName() + ", took " + timeRunning() + "ms ->" + status());
    }
  }

  public long timeRunning() {
    return isComplete() ? (stopTime - startTime) : isStarted() ? clock.now() - startTime : 0;
  }

  public Status status() {
    return !isStarted() ? Status.READY : !isComplete() ? Status.RUNNING : isSuccessful() ? Status.SUCCESS : Status.ERROR;
  }

  protected void sleep() throws InterruptedException {
    clock.sleep(completionPollIntervalMillis);
  }

  @Override
  public String toString() {
    return getName() + ": " + status();
  }

  public String fullStatus() {
    return status().explain(this);
  }

}
