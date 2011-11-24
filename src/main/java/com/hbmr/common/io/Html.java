package com.hbmr.common.io;

import java.io.IOException;
import java.io.Writer;

import com.google.common.base.Joiner;

/**
 * Html fixture
 *
 * @author Vlad Patryshev
 */
public class Html {
  private Writer out;
  private boolean atHead;
  private boolean atBody;

  public Html(Writer out) throws IOException {
    this.out = out;
    out.write("<html>");
  }

  public Html head() throws IOException {
    if (!atHead) out.write("<head>");
    atHead = true;
    return this;
  }

  public Html refresh(int seconds) throws IOException {
    head();
    return write("<META HTTP-EQUIV=\"REFRESH\" CONTENT=\"" + seconds + "\">");
  }

  public Html title(String title) throws IOException {
    head();
    return write("<title>" + title + "</title>");
  }

  public Html body() throws IOException {
    closeHead();
    atHead = false;
    atBody = true;
    return write("<body>");
  }

  private Html closeHead() throws IOException {
    if (atHead) {
      out.write("</head>");
    }
    return this;
  }

  public Html write(String s) throws IOException {
    out.write(s);
    return this;
  }

  public Html writeLines(String s) throws IOException {
    return write(s.replaceAll("\n", "<br/>"));
  }

  public void close() throws IOException {
    closeHead();
    if (atBody) {
      out.write("</body>");
    }
    out.write("</html>");
    out.close();
  }

  public Html row(Object... data) throws IOException {
    return write("<tr><td>" + Joiner.on("</td><td>").join(data) + "</td></tr>");
  }

  public Html openTable(Object... columnTitles) throws IOException {
    return write("<table><tr><th>" + Joiner.on("</th><th>").join(columnTitles) + "</th></tr>");
  }

  public Html closeTable() throws IOException {
    return write("</table>");
  }

  public String ref(String url, Object text) throws IOException {
    return "<a href=\"" + url + "\">" + text + "</a>";
  }
}
