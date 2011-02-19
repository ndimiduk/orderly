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

import com.gotometrics.util.DateUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode a java.sql.Date into an immutable byte array. Currently
 * this is done by converting the Date into a 64-bit signed long value 
 * representing milliseconds since the epoch. The long value is then 
 * converted into an immutable byte array as described in LongFormat. 
 * DateFormat preserves ascending sort ordering for Date values. 
 */
public class DateFormat extends DataFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.format.FormatUtils
   */
  protected DateFormat() { }

  @Override
  public Order getOrder() { return Order.ASCENDING; }

  @Override
  public int length(ImmutableBytesWritable bytes) { return Bytes.SIZEOF_LONG; }

  @Override
  public void encodeString(final String s, final ImmutableBytesWritable ibw) {
    try {
      encodeDate(new Date(DateUtils.parse(s).getTime()), ibw);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void encodeLong(final long l, final ImmutableBytesWritable ibw) {
    encodeDate(new Date(l), ibw);
  }

  @Override 
  public void encodeDate(final Date date, final ImmutableBytesWritable ibw) {
    long val = date.getTime() ^ Long.MIN_VALUE;
    ibw.set(Bytes.toBytes(val));
  }

  @Override
  public String decodeString(final ImmutableBytesWritable bytes) {
    return DateUtils.format(decodeDate(bytes));
  }

  @Override
  public long decodeLong(final ImmutableBytesWritable bytes) {
    return decodeDate(bytes).getTime();
  }

  @Override
  public Date decodeDate(final ImmutableBytesWritable bytes) {
    long time = Bytes.toLong(bytes.get(), bytes.getOffset()) ^ Long.MIN_VALUE;
    return new Date(time);
  }

  private static final DataFormat format = new DateFormat();
  public static DataFormat get() { return format; }
}
