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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serialize and deserialize unsigned integers into fixed-width, sortable 
 * byte arrays. The serialization and deserialization method are identical to 
 * {@link FixedIntWritableRowKey}, except the sign bit of the integer is not
 * negated before deserialization.
 *
 * <h1> NULL </h1>
 * Like all fixed-width integer types, this class does <i>NOT</i> support null
 * value types. If you need null support use @{link UnsignedIntWritableRowKey}.
 *
 * <h1> Descending sort </h1>
 * To sort in descending order we perform the same encodings as in ascending 
 * sort, except we logically invert (take the 1's complement of) each byte. 
 *
 * <h1> Usage </h1>
 * This is the fastest class for storing fixed width 32-bit unsigned ints. Use 
 * @{link UnsignedIntWritableRowKey} for a more compact, variable-length 
 * representation if integers are likely to fit into 28 bits.
 */
public class FixedUnsignedIntWritableRowKey extends FixedIntWritableRowKey
{
  protected IntWritable invertSign(IntWritable iw) {
    iw.set(iw.get() ^ Integer.MIN_VALUE);
    return iw;
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
    invertSign((IntWritable)o);
    super.serialize(o, w);
    invertSign((IntWritable)o);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    return invertSign((IntWritable) super.deserialize(w));
  }
}
