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

import java.sql.Timestamp;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode a java.sql.Timestamp into an immutable byte array. 
 * Currently this is done by converting the Timestamp into a 64-bit signed long
 * value representing milliseconds since the epoch, and a 32-bit signed
 * integer value representing the number of nanoseconds. The long value is  
 * converted into an immutable byte array as described in DescendingLongFormat,
 * and the integer value is appended to the byte array using the conversion 
 * method described in DescendingIntFormat. TimeFormat preserves descending 
 * sort ordering for Timestamp values. 
 */

public class DescendingTimestampFormat extends TimestampFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.hbase.format.FormatUtils
   */
  protected DescendingTimestampFormat() { }

  @Override
  public Order getOrder() { return Order.DESCENDING; }

  @Override
  public void encodeTimestamp(final Timestamp ts, 
                              final ImmutableBytesWritable ibw) 
  {
    long revMillis = ~(ts.getTime() ^ Long.MIN_VALUE);
    int  revNanos  = ~(ts.getNanos() ^ Integer.MIN_VALUE);

    byte[] b = new byte[Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT];
    Bytes.putLong(b, 0, revMillis);
    Bytes.putInt(b, Bytes.SIZEOF_LONG, revNanos);

    ibw.set(b);
  }

  @Override
  public Timestamp decodeTimestamp(final ImmutableBytesWritable bytes) {
    byte[] b = bytes.get();
    int offset = bytes.getOffset();
    long millis = ~Bytes.toLong(b, offset) ^ Long.MIN_VALUE;
    int nanos = ~Bytes.toInt(b, offset + Bytes.SIZEOF_LONG) ^ Integer.MIN_VALUE;

    Timestamp ts = new Timestamp(millis);
    try {
      ts.setNanos(nanos);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Corrupt Timestamp", e);
    }
    return ts;
  }

  private static final DataFormat format = new DescendingTimestampFormat();
  public static DataFormat get() { return format; }
} 
