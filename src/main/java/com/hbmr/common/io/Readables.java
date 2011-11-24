package com.hbmr.common.io;

import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * All kinds of reading functionality (Reader monad, eh) that is missing in Java
 * can go here.
 *
 * @author vlad
 */
public class Readables {
  public final static Pattern CRLF = Pattern.compile("\n\r|\r\n|\n");

  /**
   * Iterates over the lines of input.
   * <p/>
   * Usage example:
   * for (String s : iterate(new FileReader("system.properties")) {
   * ...
   * }
   *
   * @param in
   * @return an iterable
   */
  public static Iterable<String> iterate(final Readable in) {
    return iterate(in, CRLF);
  }

  /**
   * Iterates over the lines of input.
   * <p/>
   * Usage example:
   * for (String s : iterate(new FileReader("system.properties", "\n")) {
   * ...
   * }
   *
   * @param in
   * @param delimiter
   * @return
   */
  public static Iterable<String> iterate(final Readable in, String delimiter) {
    return iterate(in, Pattern.compile(delimiter));
  }

  /**
   * Iterates over the lines of input.
   * <p/>
   * Usage example:
   * for (String s : iterate(new FileReader("system.properties", Readables.CRLF)) {
   * ...
   * }
   *
   * @param in
   * @param delimiter
   * @return
   */
  public static Iterable<String> iterate(final Readable in, final Pattern delimiter) {
    return new Iterable<String>() {

      @Override
      public Iterator<String> iterator() {
        return new Scanner(in).useDelimiter(delimiter);
      }
    };
  }
}
