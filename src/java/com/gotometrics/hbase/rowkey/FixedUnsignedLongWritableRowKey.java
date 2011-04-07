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

package com.gotometrics.hbase.rowkey;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serialize and deserialize unsigned long integers into fixed-width, sortable 
 * byte arrays. The serialization and deserialization method are identical to 
 * {@link FixedLongWritableRowKey}, except the sign bit of the long is not
 * negated before deserialization.
 *
 * <h1> NULL </h1>
 * Like all fixed-width integer types, this class does <i>NOT</i> support null
 * value types. If you need null support use @{link UnsignedLongWritableRowKey}.
 *
 * <h1> Descending sort </h1>
 * To sort in descending order we perform the same encodings as in ascending 
 * sort, except we logically invert (take the 1's complement of) each byte. 
 *
 * <h1> Usage </h1>
 * This is the fastest class for storing fixed width 64-bit unsigned ints. Use 
 * @{link UnsignedLongWritableRowKey} for a more compact, variable-length 
 * representation if integers are likely to fit into 59 bits.
 */
public class FixedUnsignedLongWritableRowKey extends FixedLongWritableRowKey
{
  protected LongWritable invertSign(LongWritable lw) {
    lw.set(lw.get() ^ Long.MIN_VALUE);
    return lw;
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
    invertSign((LongWritable)o);
    super.serialize(o, w);
    invertSign((LongWritable)o);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    return invertSign((LongWritable) super.deserialize(w));
  }
}
