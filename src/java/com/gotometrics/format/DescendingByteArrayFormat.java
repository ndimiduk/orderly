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

import java.util.Arrays;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode a raw byte array. */
public class DescendingByteArrayFormat extends ByteArrayFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.hbase.format.FormatUtils
   */
  protected DescendingByteArrayFormat() { }

  @Override
  public Order getOrder() { return Order.DESCENDING; }

  @Override
  public void encodeByteArray(final byte[] a, final ImmutableBytesWritable ibw) 
  {
    byte[] b = new byte[a.length];
    for (int i = 0; i < b.length; i++)
      b[i] =  (byte) ~a[i];
    ibw.set(NullUtils.encode(getOrder(), b));
  }

  @Override
  public byte[] decodeByteArray(final ImmutableBytesWritable ibw) {
    byte[] b = NullUtils.decode(getOrder(), ByteUtils.toBytes(ibw));
    for (int i = 0; i < b.length; i++)
      b[i] = (byte) ~b[i];
    return b;
  }

  private static final DataFormat format = new DescendingByteArrayFormat();
  public static DataFormat get() { return format; }
}
