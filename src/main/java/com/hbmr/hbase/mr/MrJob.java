package com.hbmr.hbase.mr;

/**
 * Mapreduce job spec
 *
 * @author Vlad Patryshev
 */
public interface MrJob {
  enum Status {
    READY,
    RUNNING,
    SUCCESS,
    ERROR {
      public String explain(MrJob job) {
        return job.explainError();
      }
    };

    public String explain(MrJob job) {
      return toString();
    }
  }

  String getName();

  void start();

  boolean isStarted();

  boolean isComplete();

  boolean isSuccessful();

  boolean waitForCompletion() throws InterruptedException;

  float progress();

  String details();

  Status status();

  String fullStatus();

  String explainError();
}
