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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode a double-precision floating point number into a
 * byte array, preserving sort order. Floating point numbers are encoded as
 * specified in IEEE 754. Specifically, each 64-bit double consists of a 
 * sign bit, 11-bit unsigned exponent encoded in offset-1023 notation, and a 
 * 52-bit significand.
 *
 * The value of a normal float is 
 * -1^{sign bit} * 2^{exponent - 1023} * 1.significand
 *
 * Much of our work is already done for us, as this notation actually already
 * preserves sort ordering in positive floating point numbers when sorted using
 * unsigned long comparison.  To see why this is, note that the exponent is 
 * encoded using an offset of 1023 so that the most negative exponent, -1023, is
 * encoded as 0, and the most positive exponent, 1023, is encoded as 0x7fe. 
 * Infinity is encoded as 0x7ff as it is larger than any finite exponent. Zero
 * is encoded using an exponent of -1023 with a significand of zero.
 * Thus the offset-1023 notation effectively encodes the signed range of 
 * exponents into an unsigned 11-bit field, all while preserving sort ordering.
 *
 * To ensure a sort-order preserving total ordering of floating point values, 
 * we need only ensure that negative numbers sort in the the exact opposite 
 * order as positive numbers (so that say, negative infinity is less than 
 * negative 1), and that all negative numbers compare less than any positive
 * numbers). To accomplish this, we invert the sign bit and then also invert the
 * exponent and significand bits if the floating point value was negative. 
 *
 * More specifically, we first convert the floating point bits to a long
 * using {@link Double.doubleToLongBits}. This also has the additional benefit 
 * of canonicalizing any NaN values. We then compute 
 *
 * l ^= (l &gt;&gt; (Long.SIZE - 1)) | Long.MIN_SIZE
 *
 * Which inverts the sign bit and XOR's all other bits with the sign bit itself.
 * The resulting integer is then converted into a byte array as described in
 * {@link Bytes.toBytes(long)}.
 *
 * This format ensures the following total ordering of floating point values:
 * Double.NEGATIVE_INFINITY &lt; -Double.MAX_VALUE &lt; ... 
 * &lt; -Double.MIN_VALUE &lt; -0.0 &lt; +0.0; &lt; Double.MIN_VALUE &lt; ...
 * &lt; Double.MAX_VALUE &lt; Double.POSITIVE_INFINITY &lt; Double.NaN
 */

public class DoubleFormat extends DataFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.hbase.format.FormatUtils
   */
  protected DoubleFormat() { }

  @Override
  public Order getOrder() { return Order.ASCENDING; }

  @Override
  public int length(ImmutableBytesWritable bytes) { return Bytes.SIZEOF_LONG; }

  @Override
  public void encodeString(final String s, final ImmutableBytesWritable ibw) 
  {
    try {
      encodeDouble(Double.parseDouble(s), ibw);
    } catch (NumberFormatException n) {
      throw new RuntimeException("Corrupt Double", n);
    }
  }

  @Override
  public void encodeLong(final long l, final ImmutableBytesWritable ibw) {
    encodeDouble((double)l, ibw);
  }

  @Override
  public void encodeInt(final int i, final ImmutableBytesWritable ibw) {
    encodeDouble(i, ibw);
  }

  @Override
  public void encodeShort(final short s, final ImmutableBytesWritable ibw) {
    encodeDouble(s, ibw);
  }

  @Override
  public void encodeDouble(final double d, ImmutableBytesWritable ibw) {
    long l = Double.doubleToLongBits(d);
    ibw.set(Bytes.toBytes(l ^ ((l >> Long.SIZE - 1) | Long.MIN_VALUE)));
  }

  @Override
  public void encodeFloat(final float f, final ImmutableBytesWritable ibw) {
    encodeDouble(f, ibw);
  }

  @Override
  public void encodeBigDecimal(final BigDecimal val, 
                               final ImmutableBytesWritable ibw) 
  {
    encodeDouble(val.doubleValue(), ibw);
  }

  @Override
  public String decodeString(final ImmutableBytesWritable bytes) {
    return Double.toString(decodeDouble(bytes));
  }

  @Override
  public long decodeLong(final ImmutableBytesWritable bytes) {
    return (long) decodeDouble(bytes);
  }

  @Override
  public int decodeInt(final ImmutableBytesWritable bytes) {
    return (int) decodeDouble(bytes);
  }

  @Override
  public short decodeShort(final ImmutableBytesWritable bytes) {
      return (short) decodeDouble(bytes);
  }

  @Override
  public double decodeDouble(final ImmutableBytesWritable bytes) {
    long l =  Bytes.toLong(bytes.get(), bytes.getOffset());
    return Double.longBitsToDouble(l ^ ((~l >> Long.SIZE - 1) | 
            Long.MIN_VALUE));
  }

  @Override
  public float decodeFloat(final ImmutableBytesWritable bytes) {
      return (float) decodeDouble(bytes);
  }

  @Override
  public BigDecimal decodeBigDecimal(final ImmutableBytesWritable bytes) {
    return new BigDecimal(decodeDouble(bytes));
  }

  private static final DataFormat format = new DoubleFormat();
  public static DataFormat get() { return format; }
} 
