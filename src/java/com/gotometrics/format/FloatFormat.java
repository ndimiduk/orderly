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

/** Decode and encode a single-precision floating point number into a
 * byte array, preserving sort order. Floating point numbers are encoded as
 * specified in IEEE 754. Specifically, each 32-bit float consists of a 
 * sign bit, 8-bit unsigned exponent encoded in offset-127 notation, and a 
 * 23-bit significand.
 *
 * The value of a normal float is 
 * -1^{sign bit} * 2^{exponent - 127} * 1.significand
 *
 * Much of our work is already done for us, as this notation actually already
 * preserves sort ordering in positive floating point numbers when sorted using
 * unsigned integer comparison.  To see why this is, note that the exponent is 
 * encoded using an offset of 127 so that the most negative exponent, -127, is
 * encoded as 0, and the most positive exponent, 127, is encoded as 254. 
 * Infinity is encoded as 255 as it is larger than any finite exponent. Zero
 * is encoded using an exponent of -127 with a significand of zero.
 * Thus the offset-127 notation effectively encodes the signed range of 
 * exponents into an unsigned 8-bit field, all while preserving sort ordering.
 *
 * To ensure a sort-order preserving total ordering of floating point values, 
 * we need only ensure that negative numbers sort in the the exact opposite 
 * order as positive numbers (so that say, negative infinity is less than 
 * negative 1), and that all negative numbers compare less than any positive
 * numbers). To accomplish this, we invert the sign bit and then also invert the
 * exponent and significand bits if the floating point value was negative. 
 *
 * More specifically, we first convert the floating point bits to an integer 
 * using {@link Float.floatToIntBits}. This also has the additional benefit of
 * canonicalizing any NaN values. We then compute 
 *
 * i ^= (i &gt;&gt; (Integer.SIZE - 1)) | Integer.MIN_SIZE
 *
 * Which inverts the sign bit and XOR's all other bits with the sign bit itself.
 * The resulting integer is then converted into a byte array as described in
 * {@link Bytes.toBytes(int)}.
 *
 * This format ensures the following total ordering of floating point values:
 * Float.NEGATIVE_INFINITY &lt; -Float.MAX_VALUE &lt; ... 
 * &lt; -Float.MIN_VALUE &lt; -0.0 &lt; +0.0; &lt; Float.MIN_VALUE &lt; ...
 * &lt; Float.MAX_VALUE &lt; Float.POSITIVE_INFINITY &lt; Float.NaN
 */

public class FloatFormat extends DataFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.hbase.format.FormatUtils
   */
  protected FloatFormat() { }

  @Override
  public Order getOrder() { return Order.ASCENDING; }

  @Override
  public int length(ImmutableBytesWritable bytes) { return Bytes.SIZEOF_INT; }

  @Override
  public void encodeString(final String s, final ImmutableBytesWritable ibw) {
    try {
      encodeFloat(Float.parseFloat(s), ibw);
    } catch (NumberFormatException n) {
      throw new RuntimeException("Corrupt Float", n);
    }
  }

  @Override
  public void encodeLong(final long l, final ImmutableBytesWritable ibw) {
    encodeFloat((float)l, ibw);
  }

  @Override
  public void encodeInt(final int i, final ImmutableBytesWritable ibw) {
    encodeFloat((float)i, ibw);
  }

  @Override
  public void encodeShort(final short s, final ImmutableBytesWritable ibw) {
    encodeFloat(s, ibw);
  }

  @Override
  public void encodeDouble(final double d, final ImmutableBytesWritable ibw) {
    encodeFloat((float)d, ibw);
  }

  @Override
  public void encodeFloat(final float f, final ImmutableBytesWritable ibw) {
    int i = Float.floatToIntBits(f);
    ibw.set(Bytes.toBytes(i ^ ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE)));
  }

  public void encodeNullableFloat(final Float f, 
      final ImmutableBytesWritable ibw) 
  {
    int i = f == null ? 0 : Float.floatToIntBits(f.floatValue());
    if (f != null)
      i =  (i ^ ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1;
    ibw.set(Bytes.toBytes(i));
  }

  @Override
  public void encodeBigDecimal(final BigDecimal val, 
                               final ImmutableBytesWritable ibw)
  {
    encodeFloat(val.floatValue(), ibw);
  }

  @Override
  public String decodeString(final ImmutableBytesWritable bytes) {
    return Float.toString(decodeFloat(bytes));
  }

  @Override
  public long decodeLong(final ImmutableBytesWritable bytes) {
    return (long) decodeFloat(bytes);
  }

  @Override
  public int decodeInt(final ImmutableBytesWritable bytes) {
    return (int) decodeFloat(bytes);
  }

  @Override
  public short decodeShort(final ImmutableBytesWritable bytes) {
      return (short) decodeFloat(bytes);
  }

  @Override
  public double decodeDouble(final ImmutableBytesWritable bytes) {
    return decodeFloat(bytes);
  }

  @Override
  public float decodeFloat(final ImmutableBytesWritable bytes) {
    int i = Bytes.toInt(bytes.get(), bytes.getOffset());
    return Float.intBitsToFloat(i ^ ((~i >> Integer.SIZE - 1) | 
            Integer.MIN_VALUE));
  }

  public Float decodeNullableFloat(final ImmutableBytesWritable bytes) {
    int i = Bytes.toInt(bytes.get(), bytes.getOffset());
    if (i == 0) 
      return null;
    else 
      i--;
    return Float.intBitsToFloat(i ^ ((~i >> Integer.SIZE -1 | 
            Integer.MIN_VALUE)));
  }

  @Override
  public BigDecimal decodeBigDecimal(final ImmutableBytesWritable bytes) {
    return new BigDecimal(decodeFloat(bytes));
  }

  private static final DataFormat format = new FloatFormat();
  public static DataFormat get() { return format; }
}
