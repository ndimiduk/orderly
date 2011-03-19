package com.gotometrics.hbase.rowkey;

import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Various utility functions for creating and manipulating row keys. */
public class RowKeyUtils 
{
  /** Shared (immutable) empty byte array singleton for use by all classes */
  public static final byte[] EMPTY = new byte[0];

  /** Convert a (byte array, offset, length) triple into a byte array,
   * copying only if necessary. No copy is performed if offset is 0 and
   * length is array.length. 
   *
   * @param b
   * @param offset
   * @param length
   * @return 
   */
  public static byte[] toBytes(byte[] b, int offset, int length) {
    if (offset == 0 && length == b.length) 
      return b;
    else if (offset == 0)
      return Arrays.copyOf(b, length);
    return Arrays.copyOfRange(b, offset, offset + length);
  }

  /** Convert an ImmutableBytesWritable to a byte array, copying only if
   * necessary.
   * 
   * @param b
   * @param offset
   * @param length
   * @return 
   */
  public static byte[] toBytes(ImmutableBytesWritable w) {
    return toBytes(w.get(), w.getOffset(), w.getLength());
  }

  /** Convert a Text object to a byte array, copying only if
   * necessary.
   * 
   * @param b
   * @param offset
   * @param length
   * @return 
   */

  public static byte[] toBytes(Text t) {
    return toBytes(t.getBytes(), 0, t.getLength());
  }

  /** Seek forward/backward within an ImmutableBytesWritable,
   * adjusting offset and length accordingly.
   * 
   * @param w
   * @param offset
   * @return 
   */
  public static void seek(ImmutableBytesWritable w, int offset) {
    w.set(w.get(), w.getOffset() + offset, w.getLength() - offset);
  }
}
