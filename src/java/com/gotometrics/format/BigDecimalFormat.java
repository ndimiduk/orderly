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

import com.gotometrics.hbase.util.BigDecimalUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode a BigDecimal into an immutable byte array. For more
 * information on the encoding format, see {@link BigDecimalUtils}.
 */

public class BigDecimalFormat extends DataFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.hbase.format.FormatUtils
   */
  protected BigDecimalFormat() { }

  @Override
  public Order getOrder() { return Order.ASCENDING; }

  @Override
  public int length(ImmutableBytesWritable bytes) { return -1; }


  @Override
  public void encodeString(final String s, final ImmutableBytesWritable ibw) {
    encodeBigDecimal(new BigDecimal(s), ibw);
  }

  @Override
  public void encodeLong(final long l, final ImmutableBytesWritable ibw) {
    encodeBigDecimal(new BigDecimal(l), ibw);
  }

  @Override
  public void encodeInt(final int i, final ImmutableBytesWritable ibw) {
    encodeBigDecimal(new BigDecimal(i), ibw);
  }

  @Override
  public void encodeShort(final short s, final ImmutableBytesWritable ibw) {
    encodeBigDecimal(new BigDecimal((int)s), ibw);
  }

  @Override
  public void encodeDouble(final double d, final ImmutableBytesWritable ibw) {
    encodeBigDecimal(new BigDecimal(d), ibw);
  }

  @Override
  public void encodeFloat(final float f, final ImmutableBytesWritable ibw) {
    encodeBigDecimal(new BigDecimal((double)f), ibw);
  }

  @Override 
  public void encodeBigDecimal(final BigDecimal val, 
                               final ImmutableBytesWritable ibw) 
  {
    ibw.set(BigDecimalUtils.toBytes(getOrder(), val));
  }

  @Override
  public String decodeString(final ImmutableBytesWritable bytes) {
    return decodeBigDecimal(bytes).toString();
  }

  @Override
  public long decodeLong(final ImmutableBytesWritable bytes) {
    return decodeBigDecimal(bytes).longValue();
  }

  @Override
  public int decodeInt(final ImmutableBytesWritable bytes) {
    return decodeBigDecimal(bytes).intValue();
  }

  @Override
  public short decodeShort(final ImmutableBytesWritable bytes) {
      return decodeBigDecimal(bytes).shortValue();
  }

  @Override
  public double decodeDouble(final ImmutableBytesWritable bytes) {
      return decodeBigDecimal(bytes).doubleValue();
  }

  @Override
  public float decodeFloat(final ImmutableBytesWritable bytes) {
      return decodeBigDecimal(bytes).floatValue();
  }

  @Override
  public BigDecimal decodeBigDecimal(final ImmutableBytesWritable bytes) {
    return BigDecimalUtils.toBigDecimal(getOrder(), bytes.get(), 
        bytes.getOffset());
  }

  private static final DataFormat format = new BigDecimalFormat();
  public static DataFormat get() { return format; }
}
