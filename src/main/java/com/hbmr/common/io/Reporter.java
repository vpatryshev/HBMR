package com.hbmr.common.io;

import java.util.Date;
import java.util.LinkedList;

import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.hbmr.common.Clock;
import com.hbmr.common.RealClock;

public class Reporter {
  private final static String TAG = "201109221926";
  private final static boolean DEBUG_ENABLED = true;

  enum Level {
    DEBUG("gray"),
    INFO("black"),
    WARN("brown"),
    ERROR("red");

    private String color;

    private Level(String color) {
      this.color = color;
    }

    String decorate(String msg) {
      return "<font color='" + color + "'>" + msg + "</font>";
    }

    boolean includes(Level other) {
      return this.ordinal() <= other.ordinal();
    }

    static Level forName(String name) {
      try {
        return valueOf(name);
      } catch (Exception x) {
        return INFO;
      }
    }
  }

  private static Function<Entry, String> AS_HTML = new Function<Entry, String>() {
    @Override
    public String apply(Entry e) {
      return e.toHtml();
    }
  };

  class Entry {
    Level level;
    Date timestamp;
    String format;
    Object[] data;

    Entry(Level level, String format, Object[] data) {
      this.level = level;
      this.timestamp = new Date(clock.now());
      this.format = format;
      this.data = data;
    }

    @Override
    public String toString() {
      return timestamp + " " + (level + "  ").substring(0, 6) + String.format(format, data);
    }

    public String toHtml() {
      return level.decorate(toString());
    }
  }

  private static final int DEFAULT_SIZE = 100;
  private static final int MAX_SIZE = 10000;
  protected LinkedList<Entry> messages = Lists.newLinkedList();
  private int size;
  private Logger logger;
  protected Clock clock = new RealClock();

  public Reporter() {
    this(DEFAULT_SIZE, null);
  }

  public Reporter(int size, Logger logger) {
    Preconditions.checkArgument(size > 0);
    this.size = size;
    this.logger = logger;
  }

  private void post(Level level, String format, Object... args) {
    messages.push(new Entry(level, format, args));
    if (messages.size() > MAX_SIZE) messages.removeLast();
  }

  public Iterable<Entry> history(final Level level, final Date since) {
    return Iterables.limit(Iterables.filter(messages, new Predicate<Entry>() {

      @Override
      public boolean apply(Entry e) {
        return e != null &&
            (level == null || level.includes(e.level)) &&
            (since == null || e.timestamp.after(since));
      }
    }), size);
  }

  @VisibleForTesting
  String since(Date since) {
    return Joiner.on("\n").join(history(Level.forName(""), since));
  }

  public Reporter error(String s, Throwable t) {
    return error(s + ": " + (t == null ? "?" : t.getMessage()));
  }

  public Reporter error(String s) {
    post(Level.ERROR, s);
//        try {
//            if (logger != null) logger.error(s);
//        } catch (Throwable x) {
//            System.out.println(s + " (and also " + x + " while reporting to " + logger);
//        }
    return this;
  }

  public Reporter warn(String format, Object... args) {
    post(Level.WARN, format, args);
    String msg = String.format(format, args);
    try {
      if (logger != null) logger.warn(msg);
    } catch (Throwable x) {
      System.out.println(msg);
      // ignore all logger things
    }
    return this;
  }

  public Reporter info(String format, Object... args) {
    post(Level.INFO, format, args);
    String msg = String.format(format, args);
    try {
      if (logger != null) logger.info(msg);
    } catch (Throwable t) {
      System.out.println(msg);
      // ignore all logger things
    }
    return this;
  }

  public Reporter debug(String format, Object... args) {
    if (DEBUG_ENABLED) {
      post(Level.DEBUG, format, args);
      String msg = String.format(format, args);
      try {
        if (logger != null) logger.debug(msg);
      } catch (Throwable t) {
        System.out.println(msg);
        // ignore all logger things
      }
    }
    return this;
  }

  @VisibleForTesting
  Reporter withClock(Clock clock) {
    this.clock = clock;
    return this;
  }

  public String toString(String level) {
    return Joiner.on("\n").join(history(Level.forName(level), new Date(0)));
  }

  public String toHtml(String level) {
    return Joiner.on("\n").join(
        Iterables.transform(history(Level.forName(level), new Date(0)),
            AS_HTML));
  }

}
