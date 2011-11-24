package com.hbmr.common.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Joiner;
import com.google.common.io.CharStreams;

public class Http {

  private static final String UTF_8 = "UTF-8";
  private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
  private static final String APPLICATION_WWW_URL_FORM_ENCODED = "application/www-url-form-encoded";
  private static final String TEXT_PLAIN = "text/plain";
  private static final int DEFAULT_TIMEOUT = 10000; // 10 seconds default connection timeout

  public static InputStream post(String url, String contentType, String content) throws IOException {
    HttpURLConnection connection = openPost(url, contentType);
    connection.setRequestProperty("Content-Length", Integer.toString(content.length()));

    Writer out = new OutputStreamWriter(connection.getOutputStream(), UTF_8);
    out.write(content);
    out.close();
    return connection.getInputStream();
  }

  private static BufferedReader readFrom(InputStream in)
      throws UnsupportedEncodingException, IOException {
    return new BufferedReader(new InputStreamReader(in, UTF_8));
  }

  public static String postAndRead(String url, String content) throws IOException {
    return readStringFrom(post(url, TEXT_PLAIN, content));
  }

  public static String postAndRead(String url, Writable content) throws IOException {
    return readStringFrom(post(url, content));
  }

  public static void respond(HttpServletResponse response, Writable content) throws IOException {
    ServletOutputStream outputStream = response.getOutputStream();
    write(outputStream, content);
  }

  private static void write(OutputStream outputStream, Writable content)
      throws IOException {
    content.writeTo(Channels.newChannel(outputStream));
  }

  public static String get(String url) throws IOException {
    return get(url, DEFAULT_TIMEOUT);
  }

  public static String get(String url, int timeout) throws IOException {
//        System.out.println("HTTP GET " + url + "...");
    HttpURLConnection connection = openGet(url);
    connection.setConnectTimeout(timeout);
    String result = readStringFrom(connection.getInputStream());
//        System.out.println("HTTP GET " + url + "->" + result);
    return result;
  }

  public static String parameters(Map<String, ?> params) {
    String paramString = wwwUrlEncode(params);
    return paramString.isEmpty() ? "" : "?" + paramString;
  }

  public static String get(String url, Map<String, ?> params) throws IOException {
    return get(url + parameters(params));
  }

  public static InputStream readFrom(String url, Map<String, ?> params) throws IOException {
    return openGet(url + parameters(params)).getInputStream();
  }

  public static String readStringFrom(InputStream s) throws IOException,
      UnsupportedEncodingException {
    return readFully(readFrom(s));
  }

  private static String readFully(BufferedReader response) throws IOException {
    return Joiner.on("\n").join(CharStreams.readLines(response));
  }

  private static HttpURLConnection openPost(String url, String contentType)
      throws IOException, MalformedURLException, ProtocolException {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", contentType);
    connection.setUseCaches(false);
    connection.setDoInput(true);
    connection.setDoOutput(true);
    return connection;
  }

  private static HttpURLConnection openGet(String url)
      throws IOException, MalformedURLException, ProtocolException {
    HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("GET");
    connection.setDoInput(true);
    connection.setDoOutput(false);
    return connection;
  }

  public static InputStream post(String url, Writable content) throws IOException {
    HttpURLConnection connection = openPost(url, APPLICATION_OCTET_STREAM);
//        connection.setRequestProperty("Content-Length", Integer.toString(content.size()));
    OutputStream out = connection.getOutputStream();
    try {
      write(out, content);
      return connection.getInputStream();
    } finally {
      out.close();
    }
  }

  public static InputStream post(String url, Map<String, String> form) throws IOException {
    HttpURLConnection connection = openPost(url, APPLICATION_WWW_URL_FORM_ENCODED);
//        connection.setRequestProperty("Content-Length", Integer.toString(content.size()));
    OutputStream out = connection.getOutputStream();
    try {
      new OutputStreamWriter(out).write(wwwUrlEncode(form));
      return connection.getInputStream();
    } finally {
      out.close();
    }
  }

  // stolen from Timur's UrlUtils
  public static String wwwUrlEncode(Map<String, ?> form) {
    try {
      List<String> formlist = new ArrayList<String>();
      for (Map.Entry<String, ?> entry : form.entrySet()) {
        Object value = entry.getValue();
        if (value != null) {
          formlist.add(entry.getKey() + "=" + URLEncoder.encode(value.toString(), "UTF-8"));
        }
      }
      return Joiner.on('&').join(formlist);
    } catch (Exception ex) {
      throw new RuntimeException(ex); // as if anybody cares about utf-8 suddenly disappearing from the face of the world
    }
  }

  public static String getParameter(HttpServletRequest rq, String name, String defaultValue) {
    String value = rq.getParameter(name);
    return value == null ? defaultValue : value;
  }

  public static long getLong(HttpServletRequest rq, String name, long defaultValue) {
    try {
      return Long.parseLong(rq.getParameter(name));
    } catch (Exception x) {
    }
    ;

    return defaultValue;
  }
}
