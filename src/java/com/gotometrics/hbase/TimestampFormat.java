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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode a java.sql.Timestamp into an immutable byte array. 
 * Currently this is done by converting the Timestamp into a 64-bit signed long
 * value representing milliseconds since the epoch, and a 32-bit signed
 * integer value representing the number of nanoseconds. The long value is  
 * converted into an immutable byte array as described in LongFormat, and the
 * integer value is appended to the byte array using the conversion method
 * described in IntFormat. TimeFormat preserves ascending sort ordering for 
 * Time values. 
 */
public class TimestampFormat extends DataFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.format.FormatUtils
   */
  protected TimestampFormat() { }

  @Override
  public Order getOrder() { return Order.ASCENDING; }

  @Override
  public int length(ImmutableBytesWritable bytes) { 
    return Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT; 
  }

  @Override
  public void encodeString(final String s, final ImmutableBytesWritable ibw) {
    encodeTimestamp(Timestamp.valueOf(s), ibw);
  }

  @Override
  public void encodeLong(final long l, final ImmutableBytesWritable ibw) {
    encodeTimestamp(new Timestamp(l), ibw);
  }

  @Override
  public void encodeTimestamp(final Timestamp ts, 
                              final ImmutableBytesWritable ibw) 
  {
    long millis = ts.getTime() ^ Long.MIN_VALUE;
    int nanos = ts.getNanos() ^ Integer.MIN_VALUE;

    byte[] bytes = new byte[Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT];
    Bytes.putLong(bytes, 0, millis);
    Bytes.putInt(bytes, Bytes.SIZEOF_LONG, nanos);

    ibw.set(bytes);
  }

  @Override
  public String decodeString(final ImmutableBytesWritable bytes) {
    return decodeTimestamp(bytes).toString();
  }

  @Override
  public long decodeLong(final ImmutableBytesWritable bytes) {
    return decodeTimestamp(bytes).getTime();
  }

  @Override
  public Timestamp decodeTimestamp(final ImmutableBytesWritable bytes) {
    byte[] b = bytes.get();
    int offset = bytes.getOffset();

    long millis = Bytes.toLong(b, offset) ^ Long.MIN_VALUE;
    int nanos = Bytes.toInt(b, offset + Bytes.SIZEOF_LONG) ^ Integer.MIN_VALUE;

    Timestamp ts = new Timestamp(millis);
    ts.setNanos(nanos);
    return ts;
  }

  private static final DataFormat format = new TimestampFormat();
  public static DataFormat get() { return format; }
}
