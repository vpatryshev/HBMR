package com.hbmr.common;

/**
 * Abstract idea of clock, good for mocking/testing
 *
 * @author Vlad Patryshev
 */
public interface Clock {
  long now();
  void sleep(long howLong) throws InterruptedException;
}
