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

public class DescendingFloatFormat extends FloatFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.hbase.format.FormatUtils
   */
  protected DescendingFloatFormat() { }

  @Override
  public Order getOrder() { return Order.DESCENDING; }

  @Override
  public void encodeFloat(final float f, final ImmutableBytesWritable ibw) {
    int i = Float.floatToIntBits(f);
    ibw.set(Bytes.toBytes(i ^ ((~i >> Integer.SIZE - 1) & Integer.MAX_VALUE)));
  }

  @Override
  public float decodeFloat(final ImmutableBytesWritable bytes) {
    int i = Bytes.toInt(bytes.get(), bytes.getOffset());
    return Float.intBitsToFloat(i ^ ((~i >> Integer.SIZE - 1) & 
            Integer.MAX_VALUE));
  }

  private static final DataFormat format = new DescendingFloatFormat();
  public static DataFormat get() { return format; }
} 
