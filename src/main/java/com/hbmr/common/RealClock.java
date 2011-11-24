package com.hbmr.common;

/**
 * Implementation of the abstract idea of clock, good for mocking/testing
 *
 * @author Vlad Patryshev
 */
public class RealClock implements Clock {
  public long now() {
    return System.currentTimeMillis();
  }

  public void sleep(long millis) throws InterruptedException {
    Thread.sleep(millis);
  }
}
