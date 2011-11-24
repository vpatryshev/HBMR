package com.hbmr.common.collect;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Default HashMap-based ({@see java.util.HashMap}) implementation of MapWithRename
 *
 * @param <K> key type
 * @param <V> value type
 * 
 * @author Vlad Patryshev
 */
public class HashMapWithRename<K, V> extends AbstractMap<K, V> implements MapWithRename<K, V> {
  private HashMap<K, V> delegate = new HashMap<K, V>();

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return delegate.entrySet();
  }

  @Override
  public void rename(K oldKey, K newKey) {
    put(newKey, remove(oldKey));
  }

}
