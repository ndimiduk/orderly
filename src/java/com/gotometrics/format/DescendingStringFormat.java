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
import org.apache.hadoop.hbase.util.Bytes;

/** Decode and encode a short into an immutable byte array. We encode
 * Strings by converting the UTF-16 Java String into a UTF-8 encoded
 * array of bytes. Each byte in the byte array is then logically negated
 * (1's complement). The resulting encoding preserves descending sort ordering.
 * This is because by default UTF-8 preserves ascending sort ordering as
 * UTF-8 guarantees that sorting byte arrays of UTF-8 strings by byte value is 
 * equivalent to lexicographically sorting the equivalent Unicode strings by 
 * Unicode code point. This is discussed further in the 
 * <a href="http://en.wikipedia.org/wiki/UTF-8"> UTF-8 Wikipedia article</a>.
 * By logically negating the resulting UTF-8 bytes, we invert the ordering
 * guarantees and thus preserve descending, rather than ascending, sort
 * ordering.
 *
 * Due to issues with String.getBytes(), this format cannot currently reuse
 * the byte arrays contained within ImmutableBytesWritable. 
 */
public class DescendingStringFormat extends StringFormat 
{
  /** This is a singleton class, instances may only be obtained by public static
   * get() method calls or static methods in com.gotometrics.hbase.format.FormatUtils
   */
  protected DescendingStringFormat() { }

  @Override
  public Order getOrder() { return Order.DESCENDING; }

  @Override
  public void encodeString(final String s, final ImmutableBytesWritable ibw) {
    byte[] b = Bytes.toBytes(s);
    for (int i = 0; i < b.length; i++)
      b[i] =  (byte) ~b[i];
    ibw.set(NullUtils.encode(getOrder(), b));
  }

  @Override
  public String decodeString(final ImmutableBytesWritable ibw) {
    byte[] b = NullUtils.decode(getOrder(), ByteUtils.toBytes(ibw));
    for (int i = 0; i < b.length; i++)
      b[i] = (byte) ~b[i];
    return Bytes.toString(b);
  }

  private static final DataFormat format = new DescendingStringFormat();
  public static DataFormat get() { return format; }
}
