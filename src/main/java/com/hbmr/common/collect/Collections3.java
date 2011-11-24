package com.hbmr.common.collect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
   * Complementary lists functionality
   *
   * @author Vlad Patryshev
   */
public class Collections3 {
  /**
     * Finds a page out of an unsorted array:
     * meaning, if the array were sorted, this would be a subarray of the sorted array from offset to offset + size - 1.
     *
     * @param <T>    data type
     * @param data   the data. Note: this array will be reshuffled.
     * @param offset offset into the (virtually) ordered array.
     * @param size   result size
     * @return a sorted array with page data
     */
  public static <T extends Comparable<T>> Collection<T> findPage(List<T> data, int offset, int size) {
    return findPage(data, comparator(data), offset, size);
  }

  public static <T> Collection<T> getPage(List<T> data, int offset, int size) {
    if (data == null || data.size() == 0) {
      Collections.emptyList();
    }
    List<T> result = new ArrayList<T>();

    int startElement = Math.max(offset >= data.size() ? data.size() : offset, 0);
    int endElement = size <= 0 ? data.size() : Math.min(data.size(), startElement + size);

    for (int i = startElement; i < endElement; i++) {
      result.add(data.get(i));
    }
    return result;
  }

  public static <T extends Comparable<T>> Comparator<T> comparator(List<T> data) {
    return new Comparator<T>() {

      @Override
      public int compare(T o1, T o2) {
        return o1 == null ? -1 : o1.compareTo(o2);
      }

    };
  }

  /**
     * Finds a page out of an unsorted array:
     * meaning, if the array were sorted, this would be a subarray of the sorted array from offset to offset + size - 1.
     *
     * @param <T>    data type
     * @param data   the data. Note: this array will be reshuffled.
     * @param offset offset into the (virtually) ordered array.
     * @param size   result size
     * @return a sorted array with page data
     */
  public static <T> Collection<T> findPage(List<T> data, Comparator<T> comparator, int offset, int size) {
    if (offset > data.size() || data.isEmpty()) {
      return Collections.emptySet();
    }
    int minAt = quickselect(data, comparator, offset);
    int maxAt = quickselect(data, comparator, minAt, data.size() - 1, size - 1);

    Set<T> result = new TreeSet<T>();
    for (T t : data.subList(minAt, maxAt + 1)) {
      result.add(t);
    }
    return result;
  }

  public static <T> int quickselect(List<T> data, Comparator<T> comparator, int k) {
    return quickselect(data, comparator, 0, data.size() - 1, k);
  }

  public static <T> int quickselect(List<T> data, Comparator<T> comparator, int left, int right, int k) {
    if (left == right) return left;
    int pivotIndex = partition(data, comparator, left, right);
    int pivotDist = pivotIndex - left;
    return k == pivotDist ? pivotIndex :
        k < pivotDist ? quickselect(data, comparator, left, pivotIndex, k) :
            quickselect(data, comparator, pivotIndex + 1, right, k - pivotDist - 1);
  }

  public static <T> void swap(List<T> data, int i, int j) {
    if (i != j) {
      T t = data.get(i);
      data.set(i, data.get(j));
      data.set(j, t);
    }
  }

  public static <T> int partition(List<T> data, Comparator<T> comparator, int left, int right) {
    int pivotIndex = left + (right - left) / 2;
    T pivotValue = data.get(pivotIndex);
    swap(data, right, pivotIndex);
    int storeIndex = left;
    for (int i = left; i < right; i++) {
      if (comparator.compare(data.get(i), pivotValue) < 0) {
        swap(data, storeIndex++, i);
      }
    }
    swap(data, right, storeIndex);
    return storeIndex;

  }

}
