package com.hbmr.common.io;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
  *  Handles resources
  *
  *  @author Vlad Patryshev
  */
public class Resources {

  public static InputStream openResource(String filename) {
    Properties p = System.getProperties();

    String folder = p.getProperty("system.property.file");
    try {
      if (folder != null) {
        return new FileInputStream(folder + "/" + filename);
      }
    } catch (IOException ioe) {
      // ignore it so far
    }
    InputStream inputStream = Resources.class.getClassLoader().getResourceAsStream(filename);
    if (inputStream == null) {
      throw new Error(filename + " not found on classpath or in " + folder);
    }
    return inputStream;
  }

}
