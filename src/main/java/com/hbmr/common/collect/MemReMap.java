package com.hbmr.common.collect;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Default HashMap-based ({@see java.util.HashMap}) implementation of ReMap
 *
 * @param <K> key type
 * @param <V> value type
 *
 * @author Vlad Patryshev
 */
public class MemReMap<K extends Comparable<K>, V> extends AbstractMap<K, V> implements ReMap<K, V> {
  private HashMap<K, V> delegate = new HashMap<K, V>();

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return delegate.entrySet();
  }

  @Override
  public V get(Object key) {
    return delegate.get(key);
  }

  @Override
  public V put(K key, V value) {
    return delegate.put(key, value);
  }

  @Override
  public V remove(Object key) {
    return delegate.remove(key);
  }

  @Override
  public void clear() {
    delegate.clear();
  }


  @Override
  public boolean rename(K oldKey, K newKey) {
    V value = delegate.remove(oldKey);
    if (value == null) {
      return false;
    }
    delegate.put(newKey, value);
    return true;
  }

  @Override
  public SortedSet<K> listForPrefix(final String prefix) {
    return Sets.newTreeSet(Iterables.filter(keySet(), new Predicate<K>() {

      @Override
      public boolean apply(K key) {
        return key.toString().startsWith(prefix);
      }
    }));
  }

  @Override
  public int size(String prefix) {
    return listForPrefix(prefix).size();
  }

}
