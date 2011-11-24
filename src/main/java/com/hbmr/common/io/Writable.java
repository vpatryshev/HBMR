package com.hbmr.common.io;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public interface Writable {
  void writeTo(WritableByteChannel channel) throws IOException;
}
