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

import com.gotometrics.hbase.util.ByteUtils;
import com.gotometrics.hbase.util.NullUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode a short into an immutable byte array. We encode
 * Strings by converting the UTF-16 Java String into a UTF-8 encoded
 * array of bytes. This encoding preserves ascending sort ordering because 
 * UTF-8 guarantees that sorting byte arrays of UTF-8 strings by byte value is 
 * equivalent to lexicographically sorting the equivalent Unicode strings by 
 * Unicode code point. This is discussed further in the 
 * <a href="http://en.wikipedia.org/wiki/UTF-8"> UTF-8 Wikipedia article</a>.
 * As a historical aside, this nifty and very useful property of UTF-8 is due to
 * Ken Thompson and Rob Pike of Unix fame.
 *
 * Due to issues with String.getBytes, this format is unable to re-use
 * byte arrays contained within ImmutableBytesWritable objects.
 */
 
public class StringFormat extends DataFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.hbase.format.FormatUtils
   */
  protected StringFormat() { }

  @Override
  public Order getOrder() { return Order.ASCENDING; }

  public byte getNull() { return (byte)0x00; }
  public byte getTerminator() { return (byte)0x01; }

  @Override
  public int length(ImmutableBytesWritable bytes) { 
    byte[] b = bytes.get();
    int i = bytes.getOffset();

    if (b[i] == getNull()) 
      return 0;

    while (b[i] != getTerminator()) i++;
    return i - bytes.getOffset();
  }

  @Override
  public void encodeString(final String s, final ImmutableBytesWritable ibw) {
    if (s == null) {
      ibw.set(new byte[] { getNull() });
      return;
    }

    byte[] rawString = Bytes.toBytes(s),
           b = Arrays.copyOf(rawString, rawString.length + 1);
    for (int i = 0; i < b.length - 1; i++) 
      b[i] += 2;
    b[b.length - 1] = getTerminator();
    ibw.set(b);
  }

  @Override
  public void encodeDate(final Date date, final ImmutableBytesWritable ibw) {
    encodeString(date.toString(), ibw);
  }

  @Override
  public void encodeTime(final Time time, final ImmutableBytesWritable ibw) {
    encodeString(time.toString(), ibw);
  }

  @Override
  public void encodeTimestamp(final Timestamp ts, 
                              final ImmutableBytesWritable ibw) 
  {
    encodeString(ts.toString(), ibw);
  }

  @Override
  public void encodeLong(final long l, final ImmutableBytesWritable ibw) {
    encodeString(Long.toString(l), ibw);
  }

  @Override
  public void encodeInt(final int i, final ImmutableBytesWritable ibw) {
    encodeString(Integer.toString(i), ibw);
  }

  @Override
  public void encodeShort(final short s, final ImmutableBytesWritable ibw) {
    encodeString(Short.toString(s), ibw);
  }

  @Override
  public void encodeDouble(final double d, final ImmutableBytesWritable ibw) {
    encodeString(Double.toString(d), ibw);
  }

  @Override
  public void encodeFloat(final float f, final ImmutableBytesWritable ibw) {
    encodeString(Float.toString(f), ibw);
  }

  @Override
  public void encodeBigDecimal(final BigDecimal val,
                               final ImmutableBytesWritable ibw) 
  {
    encodeString(val.toString(), ibw);
  }

  @Override
  public String decodeString(final ImmutableBytesWritable ibw) 
  {
    byte[] a = ibw.get();
    int offset = ibw.getOffset();

    if (a[offset] == getNull())
      return null;

    int len = length(ibw);
    byte[] b = new byte[len];
    for (int i = 0; i < b.length; i++)
      b[i] = (byte) (a[i + offset] - 2);
    return Bytes.toString(b);
  }

  @Override
  public Date decodeDate(final ImmutableBytesWritable bytes) {
    return Date.valueOf(decodeString(bytes));
  }

  @Override
  public Time decodeTime(final ImmutableBytesWritable bytes) {
    return Time.valueOf(decodeString(bytes));
  }

  @Override
  public Timestamp decodeTimestamp(final ImmutableBytesWritable bytes) {
    return Timestamp.valueOf(decodeString(bytes));
  }

  @Override
  public long decodeLong(final ImmutableBytesWritable bytes) {
    return Long.parseLong(decodeString(bytes).trim());
  }

  @Override
  public int decodeInt(final ImmutableBytesWritable bytes) {
    return Integer.parseInt(decodeString(bytes).trim());
  }

  @Override
  public short decodeShort(final ImmutableBytesWritable bytes) {
    return Short.parseShort(decodeString(bytes).trim());
  }

  @Override
  public double decodeDouble(final ImmutableBytesWritable bytes) {
    return Double.parseDouble(decodeString(bytes).trim());
  }

  @Override
  public float decodeFloat(final ImmutableBytesWritable bytes) {
    return Float.parseFloat(decodeString(bytes).trim());
  }

  @Override
  public BigDecimal decodeBigDecimal(final ImmutableBytesWritable bytes) {
    return new BigDecimal(decodeString(bytes).trim());
  }

  private static final DataFormat format = new StringFormat();
  public static DataFormat get() { return format; }
}
