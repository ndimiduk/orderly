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

package com.gotometrics.format;

import com.gotometrics.util.IntUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode a short into an immutable byte array. Short values
 * are stored by computing value ^ Short.MIN_VALUE and
 * encoding the result into a 2-entry byte array in big endian order. We
 * take an exclusive-OR against Short.MIN_VALUE (which has the value -2^15) 
 * to preserve ascending sort ordering. 
 *
 * HBase sorts row keys using a byte array comparator. Thus, if shorts are
 * encoded as big-endian, unsigned values, then the HBase row key sort will
 * correctly order the short values. However, this encoding must support
 * signed short values. Thus we must remap the negative numbers to the lower
 * half of the unsigned short values, and the positive numbers to the upper 
 * half of the unsigned short values. This can be accomplished by flipping 
 * the sign bit (xor'ing with MIN_VALUE).
 *
 * Rememer that a short in 2's complement is:
 * -2^15 * b_15 + 2^14 * b_14 + 2^13 * b_13 + .. + 2^0 * b_0
 * 
 * Flipping the sign bit maps negative numbers to the range (0...2^15 - 1),
 * 0 becomes 2^15, and positive numbers are (2^15 + 1 ... 2^16 - 1)
 * This maintains the correct total ordering of numbers 
 * (negative &lt; zero &lt; positive) when HBase sorts. Note that in this 
 * encoding ordering is also maintained within positive and negative
 * numerical ranges as well (e.g., -2^31 &lt; -1 and 1 &lt; 2);
 */

public class ShortFormat extends DataFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.format.FormatUtils
   */
  protected ShortFormat() { }

  @Override
  public Order getOrder() { return Order.ASCENDING; }

  @Override
  public int length(ImmutableBytesWritable bytes) { 
    return IntUtils.decodeVarIntLength(bytes.get()[bytes.getOffset()]);
  }

  @Override
  public void encodeString(final String s, final ImmutableBytesWritable ibw) {
    try {
      encodeShort(Short.parseShort(s), ibw);
    } catch (NumberFormatException n) {
      throw new RuntimeException("Corrupt Short", n);
    }
  }

  @Override
  public void encodeLong(final long l, final ImmutableBytesWritable ibw) {
    encodeShort((short)l, ibw);
  }

  @Override
  public void encodeInt(final int i, final ImmutableBytesWritable ibw) {
    encodeShort((short)i, ibw);
  }

  @Override
  public void encodeShort(final short s, final ImmutableBytesWritable ibw) {
    byte[] b = new byte[IntUtils.getVarIntLength(s)];
    IntUtils.writeVarInt(s, b, 0);
    ibw.set(b);
  }

  @Override
  public void encodeDouble(final double d, final ImmutableBytesWritable ibw) {
    encodeShort((short)d, ibw);
  }

  @Override
  public void encodeFloat(final float f, final ImmutableBytesWritable ibw) {
    encodeShort((short)f, ibw);
  }

  @Override
  public void encodeBigDecimal(final BigDecimal val, 
                               final ImmutableBytesWritable ibw) 
  {
    encodeShort(val.shortValue(), ibw);
  }

  @Override
  public String decodeString(final ImmutableBytesWritable bytes) {
    return Short.toString(decodeShort(bytes));
  }

  @Override
  public long decodeLong(final ImmutableBytesWritable bytes) {
    return decodeShort(bytes);
  }

  @Override
  public int decodeInt(final ImmutableBytesWritable bytes) {
    return decodeShort(bytes);
  }

  @Override
  public short decodeShort(final ImmutableBytesWritable bytes) {
    return (short) IntUtils.readVarInt(bytes.get(), bytes.getOffset());
  }

  @Override
  public double decodeDouble(final ImmutableBytesWritable bytes) {
    return (double) decodeShort(bytes);
  }

  @Override
  public float decodeFloat(final ImmutableBytesWritable bytes) {
    return (float) decodeShort(bytes);
  }

  @Override
  public BigDecimal decodeBigDecimal(final ImmutableBytesWritable bytes) {
    return new BigDecimal((int)decodeShort(bytes));
  }

  private static final DataFormat format = new ShortFormat();
  public static DataFormat get() { return format; }
}
