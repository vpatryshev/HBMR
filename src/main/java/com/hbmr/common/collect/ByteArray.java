package com.hbmr.common.collect;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A rich object that handles all kinds of operations with byte array similar to String (implementing CharSequence);
 * it is also a List of Bytes, so one can navigate it.
 * <p/>
 * HBase, unfortunately, passes around a lot of byte arrays, offsets, lengths... does not make sense, we can replace
 * all of them with ByteArrays.
 * <p/>
 * Besides, in HBaseReplication, we will need to efficiently read files, and ByteArray can do it too.
 * <p/>
 * This is an immutable class... except that of course you can always cheat, but it is not nice.
 *
 * @author Vlad Patryshev
 */
public class ByteArray implements List<Byte>, Comparable<ByteArray>, CharSequence {

  private static final long serialVersionUID = -3654555805387023818L;
  /**
   * The underlying byte storage
   */
  private ByteBuffer storage;
  /**
   * cached hashCode
   */
  private long hashCode;
  private boolean haveHash;

  private static ByteBuffer slice(ByteBuffer buf, int offset, int length) {
    if (buf == null) {
      return null;
    }
    ByteBuffer slice = buf.duplicate();
    slice.position(offset);
    slice = slice.slice();
    slice.limit(length);
    return slice;
  }

  public ByteArray(ByteBuffer storage) {
    this(storage, 0, storage.limit());
  }

  /**
   * Constructor
   *
   * @param storage where the bytes are
   * @param offset  offset into the byte storage
   * @param length  number of bytes in the new ByteArray
   */
  public ByteArray(ByteBuffer storage, int offset, int length) {
    this.storage = slice(storage, offset, length);
  }

  /**
   * Constructor
   *
   * @param array  where the bytes are
   * @param offset offset into the byte storage
   * @param length number of bytes in the new ByteArray
   */
  public ByteArray(byte[] array, int offset, int length) {
    this(array == null ? null : ByteBuffer.wrap(array), offset, length);
  }

  /**
   * Constructor
   *
   * @param bytes where the bytes are
   */
  public ByteArray(byte... bytes) {
    this(bytes, 0, bytes == null ? 0 : bytes.length);
  }

  /**
   * Constructor
   * Builds a byte array out of a string Note that this charAt() will not
   * return characters of this string... unless you have a pure low ASCII
   * string.
   *
   * @param source string
   */
  public ByteArray(String source) {
    this(bytes(source));
  }

  public ByteArray(int n) {
    this(bufferOfInt().putInt(n));
  }

  public ByteArray(long n) {
    this(bufferOfLong().putLong(n));
  }

  private static ByteBuffer bufferOfInt() {
    return ByteBuffer.allocate(Integer.SIZE / 8);
  }

  private static ByteBuffer bufferOfLong() {
    return ByteBuffer.allocate(Long.SIZE / 8);
  }

  private static byte[] bytes(String s) {
    try {
      return s == null ? null : s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new Error("Something's wrong with this computer, it has no UTF-8", e);
    }
  }

  /**
     * {@see java.util.List}
     */
  @Override
  public int size() {
    return isNull() ? 0 : storage.limit();
  }

  /**
     * {@see java.util.List}
     */
  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  public boolean isNull() {
    return storage == null;
  }

  /**
     * {@see java.util.List}
     */
  @Override
  public boolean contains(Object o) {
    if (!(o instanceof Byte)) {
      return false;
    }
    return indexOf((Byte) o) >= 0;
  }

  /**
     * {@see java.util.List}
     */
  @Override
  public Iterator<Byte> iterator() {
    return listIterator();
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public boolean add(Byte e) {
    throw new UnsupportedOperationException();
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  /**
   * Operation not supported
   * {@see java.util.List}
   */
  @Override
  public boolean addAll(Collection<? extends Byte> c) {
    throw new UnsupportedOperationException();
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public boolean addAll(int index, Collection<? extends Byte> c) {
    throw new UnsupportedOperationException();
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }


  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  /**
     * {@see java.util.List}
     */
  @Override
  public Byte get(int index) {
    return storage.get(index);
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public Byte set(int index, Byte element) {
    throw new UnsupportedOperationException();
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public void add(int index, Byte element) {
    throw new UnsupportedOperationException();
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public Byte remove(int index) {
    throw new UnsupportedOperationException();
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public int indexOf(Object o) {
    throw new UnsupportedOperationException();
  }

  /**
     * Returns the index of the first byte in this array that is equal to b
     *
     * @param b the byte we are looking for
     * @return the index, or -1 if none found
     */
  public int indexOf(Byte b) {
    return indexOf(b.byteValue());
  }

  /**
     * Returns the index of the first byte in this array that is equal to b
     *
     * @param b the byte we are looking for
     * @return the index, or -1 if none found
     */
  public int indexOf(byte b) {
    return indexOf(b, 0);
  }

  /**
     * Returns the index of the first byte in this array that is equal to b
     *
     * @param b         the byte we are looking for
     * @param fromIndex the position starting from which we search
     * @return the index, or -1 if none found
     */
  public int indexOf(byte b, int fromIndex) {
    for (int i = fromIndex; i < size(); i++) {
      if (storage.get(i) == b) {
        return i;
      }
    }
    return -1;
  }

  /**
     * Returns the index of the last byte in this array that is equal to b
     *
     * @param b the byte we are looking for
     * @return the index, or -1 if none found
     */
  public int lastIndexOf(byte b) {
    for (int i = size() - 1; i >= 0; i--) {
      if (storage.get(i) == b) {
        return i;
      }
    }
    return -1;
  }

  static long pow(int n) {
    return pow(HASH_PRIME, n);
  }

  static long pow(long l, int n) {
    long result = 1;
    for (int i = 0; i < n; i++) {
      result *= l;
    }
    return result;
  }

  /**
   * Rabin-Karp string search {@see
   * http://en.wikipedia.org/wiki/Rabin%E2%80%93Karp_string_search_algorithm}
   *
   * @param subarray the one we are looking for
   * @return the position at which the subarray starts in current array
   */
  public int indexOf(ByteArray subarray) {
    return indexOf(subarray, 0);
  }

  /**
     * Rabin-Karp string search {@see
     * http://en.wikipedia.org/wiki/Rabin%E2%80%93Karp_string_search_algorithm}
     *
     * @param subarray  the one we are looking for
     * @param fromIndex position starting from which we do the search
     * @return the position at which the subarray starts in current array
     */
  public int indexOf(ByteArray subarray, int fromIndex) {
    int n = subarray.size();
    if (n == 0) {
      return fromIndex;
    }
    if (n + fromIndex > size()) {
      return -1;
    }
    long hash = subarray.longHashCode();
    long rollingHash = longHashCode(fromIndex, n);
    long m = pow(n - 1);
    int i = fromIndex;

    while (hash != rollingHash || !subarrayEquals(subarray, i)) {
      if (i + n == size()) {
        return -1;
      }

      rollingHash = (rollingHash - get(i) * m) * HASH_PRIME + get(i + n);
      i++;
    }
    return i;
  }

  /**
     * Rabin-Karp string search {@see
     * http://en.wikipedia.org/wiki/Rabin%E2%80%93Karp_string_search_algorithm}
     * This one searches from end to beginning
     *
     * @param subarray the one we are looking for
     * @return the position at which the subarray starts in current array
     */
  public int lastIndexOf(ByteArray subarray) {
    int n = subarray.size();
    if (n == 0) {
      return 0;
    }
    if (n > size()) {
      return -1;
    }
    long hash = subarray.longHashCode();
    long rollingHash = longHashCode(size() - n, n);
    long m = pow(n - 1);
    int i = size() - n;
    while (hash != rollingHash || !subarrayEquals(subarray, i)) {
      if (i <= 0) {
        return -1;
      }
      long cm = get(i - 1) * m;
      rollingHash = (rollingHash - get(i + n - 1)) * INVERSE_HASH_PRIME + cm;
      i--;
    }
    return i;
  }

  private boolean subarrayEquals(ByteArray subarray, int offset) {
    return offset >= 0
        && offset + subarray.size() <= size()
        && (subarray.size() == 0 || subarray.equals(slice(offset, offset + subarray.size())));
  }

  /**
     * Checks whether this ByteArray has another as a prefix
     *
     * @param prefix
     * @return true if this buffer's bytes start with the bytes of the prefix buffer
     */
  public boolean startsWith(ByteArray prefix) {
    return subarrayEquals(prefix, 0);
  }

  /**
     * Checks whether this ByteArray has another as a suffix
     *
     * @param suffix
     * @return true if this buffer's bytes end with the bytes of the prefix buffer
     */
  public boolean endsWith(ByteArray suffix) {
    return subarrayEquals(suffix, size() - suffix.size());
  }

  /**
     * Operation not supported
     * {@see java.util.List}
     */
  @Override
  public int lastIndexOf(Object o) {
    throw new UnsupportedOperationException();
  }


  /**
     * {@see java.util.List}
     */
  @Override
  public ListIterator<Byte> listIterator() {
    return listIterator(0);
  }

  /**
     * {@see java.util.List}
     */
  @Override
  public ListIterator<Byte> listIterator(final int index) {
    final ByteBuffer buf = ((ByteBuffer) storage.duplicate().position(index));

    return new ListIterator<Byte>() {

      @Override
      public boolean hasNext() {
        return buf.remaining() > 0;
      }

      @Override
      public Byte next() {
        return buf.get();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean hasPrevious() {
        return buf.position() > 0;
      }

      @Override
      public Byte previous() {
        buf.position(previousIndex());
        return buf.get(buf.position());
      }

      @Override
      public int nextIndex() {
        return buf.position();
      }

      @Override
      public int previousIndex() {
        return buf.position() - 1;
      }

      @Override
      public void set(Byte e) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void add(Byte e) {
        throw new UnsupportedOperationException();
      }

    };
  }

  /**
     * Builds another ByteArray, as a slice of this one.
     * No array memory allocation, just a mapping.
     *
     * @param fromIndex position of the first byte to include
     * @param toIndex   the last byte of the slice (not included)
     * @return a new ByteArray.
     */
  public ByteArray slice(int fromIndex, int toIndex) {
    checkIndex(fromIndex, "fromIndex");
    checkIndex(toIndex, "toIndex");
    return new ByteArray(storage, fromIndex, toIndex - fromIndex);
  }


  /**
     * {@see java.util.List}
     */
  @Override
  public List<Byte> subList(int fromIndex, int toIndex) {
    return slice(fromIndex, toIndex);
  }

  private void checkIndex(int index, String message) {
    if (index < 0 || index > size()) {
      throw new IndexOutOfBoundsException(message + "=" + index
          + " out of bounds(0, " + size() + ")");
    }
  }

  /**
     * Compares this ByteArray with another, byte by byte.
     */
  @Override
  public int compareTo(ByteArray that) {
    for (int i = 0; i < Math.min(size(), that.size()); i++) {
      int c = get(i) - that.get(i);
      if (c != 0) {
        return c < 0 ? -1 : 1;
      }
    }
    return size() == that.size() ? 0 : size() < that.size() ? -1 : 1;
  }

  /**
     * Same as size()
     */
  @Override
  public int length() {
    return size();
  }

  /**
     * As a CharSequence, it returns a "character" at given position.
     * The character is actually a byte; no encoding involved,
     * and if you had a UTF-8 or a UTF-16 as a source of this ByteBuffer,
     * you won't get those encoded characters, but just bytes.
     * {@see CharSequence}
     */
  @Override
  public char charAt(int index) {
    return (char) get(index).byteValue();
  }

  @Override
  public int hashCode() {
    return (int) longHashCode();
  }

  private long longHashCode() {
    if (!haveHash) {
      hashCode = longHashCode(0, size());
      haveHash = true;
    }
    return hashCode;
  }

  final static long HASH_PRIME = 1021;
  private static final long INVERSE_HASH_PRIME = 7497942008412795221L;

  long longHashCode(int from, int n) {
    long code = 0;
    for (int i = from; i < from + n; i++) {
      code = code * HASH_PRIME + get(i);
    }
    return code;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof ByteArray
        && this.compareTo(((ByteArray) obj)) == 0;
  }

  @Override
  public String toString() {
    return new StringBuilder(this).toString();
  }

  /**
   * {@see CharSequence}
   */
  @Override
  public CharSequence subSequence(int start, int end) {
    return slice(start, end);
  }

  /**
   * {@see ByteBuffer}
   *
   * @return the underlying array (which may not exist)
   */
  public byte[] array() {
    return storage.array();
  }

  /**
   * {@see ByteBuffer}
   *
   * @return offset in the underlying array (which may not exist)
   */
  public int arrayOffset() {
    return storage.arrayOffset();
  }

  /**
   * @return an array of bytes out of this ByteArray. This operation is not efficient; use with care.
   */
  public byte[] getBytes() {
    byte[] bytes = new byte[size()];
    storage.duplicate().get(bytes);
    return bytes;
  }

  /**
   * Concatenates this byte array with another.
   *
   * @param other
   * @return
   */
  public ByteArray concat(ByteArray other) {
    return concat(this, other);
  }

  /**
   * Concatenates a bunch (vararg) of byte arrays; memory is allocated on heap
   *
   * @param arrays
   * @return concatenated array
   */
  public static ByteArray concat(ByteArray... arrays) {
    return concat(Lists.newArrayList(arrays));
  }


  /**
   * Concatenates a bunch (iterable) of byte arrays; memory is allocated on heap
   * TODO(vlad): use a class from myjavatools.com to have virtual concatenation
   *
   * @param arrays
   * @return concatenated array
   */
  public static ByteArray concat(Iterable<ByteArray> arrays) {
    int length = 0;
    for (ByteArray ba : arrays) {
      if (ba != null && !ba.isNull()) {
        length += ba.length();
      }
    }

    ByteBuffer buf = ByteBuffer.allocate(length);

    for (ByteArray ba : arrays) {
      if (ba != null && !ba.isNull()) {
        buf.put(ba.storage.duplicate()); // duplicate, or else position is moved ahead
      }
    }

    return new ByteArray(buf);
  }

  /**
   * Writes this array
   *
   * @param channel
   * @throws IOException
   */
  public void writeTo(WritableByteChannel channel) throws IOException {
    writeInt(channel, length());
    dumpTo(channel);
  }

  public ReadableByteChannel readFrom() {
    return Channels.newChannel(new ByteArrayInputStream(getBytes()));
  }

  /**
   * Dumps the bytes of this array to the given channel. Does not write the length, so beware!
   *
   * @param channel
   * @throws IOException
   */
  public void dumpTo(WritableByteChannel channel) throws IOException {
    channel.write(storage.duplicate());
  }

  public static ByteArray readFrom(ReadableByteChannel channel, int limit) throws IOException {
    int size = readInt(channel);
    if (size < 0) {
      throw new IOException("Wrong size (" + size + ")");
    }
    if (size > limit) {
      throw new IOException("Wrong size (" + size + "), max " + limit);
    }
    ByteBuffer storage = ByteBuffer.allocate(size);
    channel.read(storage);
    storage.flip();
    return new ByteArray(storage);
  }

  public static ByteArray readFrom(ReadableByteChannel channel) throws IOException {
    int size = readInt(channel);
    if (size < 0) {
      throw new IOException("Wrong size (" + size + ")");
    }
    ByteBuffer storage = ByteBuffer.allocate(size);
    channel.read(storage);
    storage.flip();
    return new ByteArray(storage);
  }

  public void expectFrom(ReadableByteChannel channel) throws IOException {
    try {
      ByteArray found = ByteArray.readFrom(channel);
      if (!equals(found)) {
        throw new IOException("Expected " + this + ", found " + found);
      }
    } catch (IllegalArgumentException iae) {
      throw new IOException("Expected " + this + ", but got nothing.");
    }
  }

  public static void writeInt(WritableByteChannel channel, int n) throws IOException {
    channel.write((ByteBuffer) (bufferOfInt().putInt(n).flip()));
  }

  public static int readInt(ReadableByteChannel channel) throws IOException {
    ByteBuffer buf = bufferOfInt();
    channel.read(buf);
    buf.flip();
    Preconditions.checkArgument(buf.limit() >= Integer.SIZE / 8, "no space for integer: " + buf.limit());
    return buf.getInt();
  }

  public static String toHexString(byte[] bytes) {
    List<String> hexes = Lists.newArrayList();
    for (byte b : bytes) {
      hexes.add(String.format("%02x", b));
    }
    return Joiner.on(" ").join(hexes);
  }

}
