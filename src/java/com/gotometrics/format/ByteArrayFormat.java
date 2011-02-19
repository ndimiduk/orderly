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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode a raw byte array. Format just applies null encoding to the
 * raw byte array.
 */
public class ByteArrayFormat extends DataFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.hbase.format.FormatUtils
   */
  protected ByteArrayFormat() { }

  @Override
  public Order getOrder() { return Order.ASCENDING; }

  @Override
  public int length(ImmutableBytesWritable bytes) { 
    return NullUtils.length(getOrder(), bytes);
  }

  @Override
  public void encodeString(final String s, final ImmutableBytesWritable ibw) {
      encodeByteArray(Base64.decode(s), ibw);
  }

  @Override 
  public void encodeByteArray(final byte[] b, final ImmutableBytesWritable ibw) 
  {
    ibw.set(NullUtils.encode(getOrder(), b));
  }

  @Override
  public String decodeString(final ImmutableBytesWritable bytes) {
    return Base64.encodeBytes(decodeByteArray(bytes));
  }

  @Override
  public byte[] decodeByteArray(final ImmutableBytesWritable bytes) {
    return NullUtils.decode(getOrder(), ByteUtils.toBytes(bytes));
  }

  private static final DataFormat format = new ByteArrayFormat();
  public static DataFormat get() { return format; }
}
