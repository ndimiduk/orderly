/* Copyright 2011 GOTO Metrics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.gotometrics.hbase.format;

import com.gotometrics.hbase.util.IntUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode an integer into an immutable byte array. Integer values
 * are stored by computing value ^ Integer.MIN_VALUE and
 * encoding the result into a 4-entry byte array in big endian order. We
 * take an exclusive-OR against Integer.MIN_VALUE (which has the value -2^31) 
 * to preserve ascending sort ordering. 
 *
 * HBase sorts row keys using a byte array comparator. Thus, if integers are
 * encoded as big-endian, unsigned values, then the HBase row key sort will
 * correctly order the integer values. However, this encoding must support
 * signed integer values. Thus we must remap the negative numbers to the lower
 * half of the unsigned integer values, and the positive numbers to the upper 
 * half of the unsigned integer values. This can be accomplished by flipping 
 * the sign bit (xor'ing with MIN_VALUE).
 *
 * Rememer that an int in 2's complement is:
 * -2^31 * b_31 + 2^30 * b_30 + 2^29 * b_29 + .. + 2^0 * b_0
 * 
 * Flipping the sign bit maps negative numbers to the range (0...2^31 - 1),
 * 0 becomes 2^31, and positive numbers are (2^31 + 1 ... 2^32 - 1)
 * This maintains the correct total ordering of numbers 
 * (negative &lt; zero &lt; positive) when HBase sorts. Note that in this 
 * encoding ordering is also maintained within positive and negative
 * numerical ranges as well (e.g., -2^31 &lt; -1 and 1 &lt; 2);
 */

public class IntFormat extends DataFormat
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.hbase.format.FormatUtils
   */
  protected IntFormat() { }

  @Override
  public Order getOrder() { return Order.ASCENDING; }

  @Override
  public int length(ImmutableBytesWritable bytes) { 
    return IntUtils.decodeVarIntLength(bytes.get()[bytes.getOffset()]);
  }

  @Override
  public void encodeString(final String s, final ImmutableBytesWritable ibw) {
    try {
      encodeInt(Integer.parseInt(s), ibw);
    } catch (NumberFormatException n) {
      throw new RuntimeException("Corrupt Integer", n);
    }
  }

  @Override
  public void encodeLong(final long l, final ImmutableBytesWritable ibw) {
    encodeInt((int)l, ibw);
  }

  @Override
  public void encodeInt(final int i, final ImmutableBytesWritable ibw) {
    byte[] b = new byte[IntUtils.getVarIntLength(i)];
    IntUtils.writeVarInt(i, b, 0);
    ibw.set(b);
  }

  @Override
  public void encodeShort(final short s, final ImmutableBytesWritable ibw) {
    encodeInt(s, ibw);
  }

  @Override
  public void encodeDouble(final double d, final ImmutableBytesWritable ibw) {
    encodeInt((int)d, ibw);
  }

  @Override
  public void encodeFloat(final float f, final ImmutableBytesWritable ibw) {
    encodeInt((int)f, ibw);
  }

  @Override
  public void encodeBigDecimal(final BigDecimal val, 
                               final ImmutableBytesWritable ibw) 
  {
    encodeInt(val.intValue(), ibw);
  }

  @Override
  public String decodeString(final ImmutableBytesWritable bytes) {
    return Integer.toString(decodeInt(bytes));
  }

  @Override
  public long decodeLong(final ImmutableBytesWritable bytes) {
    return decodeInt(bytes);
  }

  @Override
  public int decodeInt(final ImmutableBytesWritable bytes) {
    return IntUtils.readVarInt(bytes.get(), bytes.getOffset());
  }

  @Override
  public short decodeShort(final ImmutableBytesWritable bytes) {
    return (short) decodeInt(bytes);
  }

  @Override
  public double decodeDouble(final ImmutableBytesWritable bytes) {
    return (double) decodeInt(bytes);
  }

  @Override
  public float decodeFloat(final ImmutableBytesWritable bytes) {
    return (float) decodeInt(bytes);
  }

  @Override
  public BigDecimal decodeBigDecimal(final ImmutableBytesWritable bytes) {
    return new BigDecimal(decodeInt(bytes));
  }

  private static final DataFormat format = new IntFormat();
  public static DataFormat get() { return format; }
}
