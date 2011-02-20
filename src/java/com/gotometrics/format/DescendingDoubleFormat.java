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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class DescendingDoubleFormat extends DoubleFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.hbase.format.FormatUtils
   */
  protected DescendingDoubleFormat() { }

  @Override
  public Order getOrder() { return Order.DESCENDING; }

  @Override
  public void encodeDouble(final double d, final ImmutableBytesWritable ibw) {
    long l = Double.doubleToLongBits(d);
    ibw.set(Bytes.toBytes(l ^ ((~l >> Long.SIZE - 1) & Long.MAX_VALUE)));
  }

  public void encodeNullableDouble(final Double d,
      final ImmutableBytesWritable ibw) 
  {
    long l = d == null ? -1 : Double.doubleToLongBits(d);
    if (d != null)
      l = (l ^ ((~l >> Long.SIZE - 1) & Long.MAX_VALUE)) - 1;
    ibw.set(Bytes.toBytes(l));
  }

  @Override
  public double decodeDouble(final ImmutableBytesWritable bytes) {
    long l = Bytes.toLong(bytes.get(), bytes.getOffset());
    return Double.longBitsToDouble(l ^ ((~l >> Long.SIZE - 1) & 
            Long.MAX_VALUE));
  }

  public Double decodeNullableDouble(final ImmutableBytesWritable bytes) {
    long l =  Bytes.toLong(bytes.get(), bytes.getOffset());
    if (l == -1) 
      return null;
    else 
      l++;
    return Double.longBitsToDouble(l ^ ((~l >> Long.SIZE - 1) & 
            Long.MAX_VALUE));
  }

  private static final DataFormat format = new DescendingDoubleFormat();
  public static DataFormat get() { return format; }
} 
