package com.hbmr.common.io;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.hbmr.common.collect.ByteArray;
import com.hbmr.common.collect.ReMap;

/**
 * Filesystem folder, aka directory
 *
 * @author Vlad Patryshev
 */
public class Folder implements ReMap<String, ByteArray> {

  @Override
  public String toString() {
    return "Folder (" + folder + ")";
  }

  public final static class Exists extends RuntimeException {
    private static final long serialVersionUID = 1L;

    Exists(Throwable cause) {
      super(cause);
    }

    Exists(String message) {
      super(message);
    }
  }

  public final static class CouldNotRead extends RuntimeException {

    private static final long serialVersionUID = 1L;

    CouldNotRead(Throwable cause) {
      super(cause);
    }

    CouldNotRead(String message) {
      super(message);
    }
  }

  public final static class CouldNotWrite extends RuntimeException {
    private static final long serialVersionUID = 1L;

    CouldNotWrite(Throwable cause) {
      super(cause);
    }

    CouldNotWrite(String message) {
      super(message);
    }
  }

  private static final FileFilter FILES_ONLY = new FileFilter() {

    @Override
    public boolean accept(File file) {
      return file.isFile() && file.canRead() && file.canWrite();
    }

  };

  private static final FileFilter DIRECTORIES_ONLY = new FileFilter() {

    @Override
    public boolean accept(File file) {
      return file.isDirectory();
    }

  };

  private File folder;

  public Folder(File folder) {
    if (!folder.exists()) {
      folder.mkdirs();
    }
    if (!folder.isDirectory()) {
      throw new Exists("File " + folder.getAbsolutePath() + " exists, but it must be a directory");
    }
    this.folder = folder;
  }

  public Folder(String name) {
    this(new File(name));
  }

  @Override
  public int size() {
    return content().size();
  }

  private SortedSet<String> content() {
    return listForPrefix("");
  }

  @Override
  public SortedSet<String> listForPrefix(String prefix) {
    if (prefix.endsWith("/")) {
      prefix = prefix.substring(0, prefix.length() - 1);
    }
    return addToContent(new File(folder, prefix), new TreeSet<String>(), prefix);
  }

  private String path(String prefix, String suffix) {
    return prefix.isEmpty() ? suffix : prefix + "/" + suffix;
  }

  private SortedSet<String> addToContent(File thisFolder, SortedSet<String> content, String prefix) {
    if (thisFolder.isDirectory()) {
      for (File file : thisFolder.listFiles(FILES_ONLY)) {
        content.add(path(prefix, file.getName()));
      }
      for (File subFolder : thisFolder.listFiles(DIRECTORIES_ONLY)) {
        addToContent(subFolder, content, path(prefix, subFolder.getName()));
      }
    }
    return content;
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    return key != null && content().contains(key);
  }

  private File fileFor(Object key) {
    return new File(folder, key.toString());
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteArray get(Object key) {
    if (!containsKey(key)) {
      return null;
    }
    try {
      FileChannel c = new RandomAccessFile(fileFor(key), "r").getChannel();
      return new ByteArray(c.map(FileChannel.MapMode.READ_ONLY, 0, c.size()));
    } catch (IOException e) {
      throw new CouldNotRead(e);
    }
  }

  @Override
  public ByteArray put(String key, ByteArray value) {
    if (key.indexOf("/") >= 0) {

      File subfolder = new File(folder, key.substring(0, key.lastIndexOf("/")));
      if (!subfolder.mkdirs() && !subfolder.isDirectory()) {
        throw new CouldNotWrite("Cannot create or open folder " + subfolder.getAbsolutePath());
      }
    }
    FileOutputStream out = null;
    try {
      out = new FileOutputStream(fileFor(key));
      value.dumpTo(out.getChannel());
    } catch (FileNotFoundException e) {
      throw new CouldNotWrite(e);
    } catch (IOException e) {
      throw new CouldNotWrite(e);
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          // could not close a file, tweet message about it
        }
      }
    }
    return null;
  }

  @Override
  public ByteArray remove(Object key) {
    if (containsKey(key)) {
      fileFor(key).delete();
    }
    return null;
  }

  @Override
  public void putAll(Map<? extends String, ? extends ByteArray> map) {
    for (String key : map.keySet()) {
      put(key, map.get(key));
    }
  }

  @Override
  public void clear() {
    for (String file : content()) {
      remove(file);
    }
  }

  @Override
  public Set<String> keySet() {
    return content();
  }

  @Override
  public Collection<ByteArray> values() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<java.util.Map.Entry<String, ByteArray>> entrySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(String oldKey, String newKey) {
    if (!containsKey(oldKey)) {
      return false;
    }
    File newFile = fileFor(newKey);
    newFile.getParentFile().mkdirs();
    return fileFor(oldKey).renameTo(newFile);
  }

  @Override
  public int size(String prefix) {
    return listForPrefix(prefix).size();
  }

}
