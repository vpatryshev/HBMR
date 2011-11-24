package com.hbmr.common.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import com.hbmr.common.collect.ByteArray;

public class Writables {

  /**
   * Given a writable, returns its bytes as a plain array
   *
   * @param x the writable
   * @return a byte array
   * @throws IOException
   */
  public static byte[] bytesOf(Writable x) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    WritableByteChannel out = Channels.newChannel(bytes);
    x.writeTo(out);
    return bytes.toByteArray();
  }

  public static ReadableByteChannel fromBytes(byte[] bytes) {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ReadableByteChannel channel = Channels.newChannel(bais);
    return channel;
  }

  public static ByteArray toByteArray(Writable x) throws IOException {
    return new ByteArray(bytesOf(x));
  }

}
