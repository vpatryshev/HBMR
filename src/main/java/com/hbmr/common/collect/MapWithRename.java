package com.hbmr.common.collect;

import java.util.Map;

/**
   * Just extends Map&lt;K,V> ({@see java.util.Map}) with one method, rename()
   * This is how file systems directories work
   *
   * @param <K>
   * @param <V>
   * @author vlad
   */
public interface MapWithRename<K, V> extends Map<K, V> {
  /**
     * Renames an entry in the map
     * Conceptually, same as put(newKey, remove(oldKey))
     *
     * @param oldKey old key for this entry
     * @param newKey new key for this entry
     */
  public void rename(K oldKey, K newKey);
}
