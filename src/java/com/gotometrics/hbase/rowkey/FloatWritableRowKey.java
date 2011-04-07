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

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Serialize and deserialize FloatWritables into HBase row keys.
 * Floating point numbers are encoded as specified in IEEE 754. A 32-bit single
 * precision float consists of a sign bit, 8-bit unsigned exponent encoded 
 * in offset-127 notation, and a 23-bit significand. The format is described
 * further in the <a href="http://en.wikipedia.org/wiki/Single_precision">
 * Single Precision Floating Point Wikipedia page</a>
 *
 * The value of a normal float is 
 * -1 <sup>sign bit</sup> &times; 2<sup>exponent - 127</sup>
 * &times; 1.significand
 *
 * The IEE754 floating point format already preserves sort ordering 
 * for positive floating point numbers when the raw bytes are compared in 
 * most significant byte order. This is discussed further at 
 * <a href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
 * http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm</a>
 *
 * Thus, we need only ensure that negative numbers sort in the the exact 
 * opposite order as positive numbers (so that say, negative infinity is less 
 * than negative 1), and that all negative numbers compare less than any 
 * positive number. To accomplish this, we invert the sign bit of all floating 
 * point numbers, and we also invert the exponent and significand bits if the 
 * floating point number was negative. 
 *
 * More specifically, we first store the floating point bits into a 32-bit 
 * integer j using {@link Float.floatToIntBits}. This method collapses all NaNs
 * into a single, canonical NaN value but otherwise leaves the bits unchanged. 
 * We then compute 
 *
 * j ^= (j &gt;&gt; (Integer.SIZE - 1)) | Integer.MIN_SIZE
 *
 * which inverts the sign bit and XOR's all other bits with the sign bit itself.
 * Comparing the raw bytes of j in most significant byte order is equivalent to
 * performing a single precision floating point comparison on the underlying
 * bits (ignoring NaN comparisons, as NaNs don't compare equal to anything when
 * performing floating point comparisons).
 *
 * Finally, we must encode NULL efficiently. Fortunately, j can never have 
 * all of its bits set to one (equivalent to -1 signed in two's complement) as 
 * this value corresponds to a NaN removed during NaN canonicalization. Thus,
 * we can encode NULL as an integer zero, and all non-NULL numbers are encoded 
 * as specified above and then incremented by 1, which is guaranteed not to 
 * cause unsigned overflow as j must have at least one bit set to zero.
 *
 * The resulting integer is then converted into a byte array by serializing
 * the integer one byte at a time in most significant byte order as described in 
 * {@link RowKeyUtils.toBytes(int, byte[], int)}. All serialized values are
 * 4 bytes in length.
 *
 * This format ensures the following total ordering of floating point values:
 * Float.NEGATIVE_INFINITY &lt; -Float.MAX_VALUE &lt; ... 
 * &lt; -Float.MIN_VALUE &lt; -0.0 &lt; +0.0; &lt; Float.MIN_VALUE &lt; ...
 * &lt; Float.MAX_VALUE &lt; Float.POSITIVE_INFINITY &lt; Float.NaN
 *
 * <h1> Descending sort </h1>
 * To sort in descending order we perform the same encodings as in ascending 
 * sort, except we logically invert (take the 1's complement of) each byte. 
 *
 * <h1> Usage </h1>
 * This is the fastest class for storing floats. It performs the no copying
 * during serialization and deserialization.
 */
public class FloatWritableRowKey extends RowKey 
{
  private static final int NULL = 0;
  private FloatWritable fw;

  @Override
  public Class<?> getSerializedClass() { return FloatWritable.class; }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    return Bytes.SIZEOF_INT;
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) 
    throws IOException
  {
    byte[] b = w.get();
    int offset = w.getOffset();
    int j;

    if (o == null) {
      j = NULL;
    } else {
      j = Float.floatToIntBits(((FloatWritable)o).get());
      j = (j ^ ((j >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1; 
    }
   
    Bytes.putInt(b, offset, j ^ order.mask());
    RowKeyUtils.seek(w, Bytes.SIZEOF_INT);
  }

  @Override
  public void skip(ImmutableBytesWritable w) throws IOException {
    RowKeyUtils.seek(w, Bytes.SIZEOF_INT);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    try {
      int offset = w.getOffset();
      byte[] s = w.get();
      int j = Bytes.toInt(s, offset) ^ order.mask();

      if (j == NULL)
        return null;

      if (fw == null)
        fw = new FloatWritable();

      j--;
      j ^= (~j >> Integer.SIZE - 1) | Integer.MIN_VALUE;
      fw.set(Float.intBitsToFloat(j));
      return fw;
    } finally {
      RowKeyUtils.seek(w, Bytes.SIZEOF_INT);
    }
  }
}
