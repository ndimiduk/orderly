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

import java.sql.Time;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode a java.sql.Time into an immutable byte array. Currently
 * this is done by converting the Time into a 64-bit signed long value 
 * representing milliseconds since the epoch. The long value is then 
 * converted into an immutable byte array as described in DescendingLongFormat. 
 * TimeFormat preserves descending sort ordering for Time values. 
 */

public class DescendingTimeFormat extends TimeFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.format.FormatUtils
   */
  protected DescendingTimeFormat() { }

  @Override
  public Order getOrder() { return Order.DESCENDING; }

  @Override
  public void encodeTime(final Time time, final ImmutableBytesWritable ibw) {
    long val = ~(time.getTime() ^ Long.MIN_VALUE);
    ibw.set(Bytes.toBytes(val));
  }

  @Override
  public Time decodeTime(final ImmutableBytesWritable bytes) {
    long time = ~Bytes.toLong(bytes.get(), bytes.getOffset()) ^ Long.MIN_VALUE;
    return new Time(time);
  }

  private static final DataFormat format = new DescendingTimeFormat();
  public static DataFormat get() { return format; }
} 
