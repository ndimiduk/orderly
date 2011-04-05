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
import org.apache.hadoop.io.Writable;

/** Serialize and deserialize signed, two's complement integers into a
 * variable-length sortable byte array representation. The basic idea is to 
 * only encode those bits that have values differing from the (explicit) sign 
 * bit. 
 * <p>
 * Our encoding consists of a header byte followed by 0-4 data bytes. The data
 * bytes are packed 8-bit data values in big-endian order. The header byte
 * contains the sign bit, the number of data bytes, and the 2-6 most significant
 * bits of data.
 * <p>
 * In the case of single byte encodings, the header byte contains 6 bits of
 * data. For double byte encodings, the header byte contains 5 bits of data, 
 * and for all other lengths the header byte contains 3 bits of data.
 * <p>
 * Thus we encode all numbers in two's complement using the sign bit in the 
 * header and 2<sup>H+D</sup> data bits, where H is the number of data bits in the 
 * header byte and D is the number of data bits in the data bytes 
 * (D = number of data bytes &times; 8). 
 * <p>
 * More specifically, the numerical ranges for our variable-length byte 
 * encoding are:
 * <ul>
 *   <li> One byte: -64 &le; x &le; 63
 *   <li> Two bytes: -8192 &le; x &le; 8191
 *   <li> N &gt; 2 bytes: -2<sup>8 &times; (N-1) + 3</sup> &le; x 
 *        &le; 2<sup>8 &times; (N-1) + 3</sup> - 1
 * </ul>
 * We support all values that can be represented in a java int, so N &le; 5.
 *
 * <h1> Reserved Bits </h1>
 *
 * Up to two of the most significant bits in the header may be reserved for use
 * by the application, as two is the minimum number of data bits in the header
 * byte. Reserved bits decrease the amount of data stored in the header byte,
 * For example, a single byte encoding with two reserved bits can only encode 
 * integers in the range -16 &le; x &le; 15.
 *
 * <h1> Header Format </h1>
 * Given an integer, x: 
 * <p>
 * sign = x &gt;&gt; Integer.SIZE - 1
 * <p>
 * negSign = ~sign
 * <p>
 * The format of the header byte is 
 * <ul>
 *   <li>Bit 7: negSign
 *   <li>Bit 6: single-byte encoded ^ negSign
 *   <li>Bit 5: double-byte encoded ^ negSign
 *   <li> Bit 3-4: len ^ sign (each bit XOR'd with original, unnegated sign bit)
 * </ul>
 *
 * Bits 6 and 7 are used in all encodings. If bit 6 indicates a single byte
 * encodng, then bits 0-5 are all data bits. Otherwise, bit 5 is used to
 * indicate a double byte encoding. If this is true, then bits 0-4 are data
 * bits. Otherwise, bits 3-4 specify the length of the multi-byte (&gt; 2)
 * encoding, with a +3 bias as described below. In all cases, bits 0-2 are 
 * data bits.
 * <p>
 * The len field represents the length of the encoded byte array minus 3.
 * In other words, the encoded len field has a bias of +3, so an encoded
 * field with value 1 is 4 decoded.
 * The XOR's with sign and negSign are required to preserve sort ordering when
 * using a big-endian byte array comparator to sort the encoded values.
 * <p>
 * Any trailing bits that are unused are padded with the sign bit. The worst 
 * case overhead versus a standard fixed-length encoding is 1 additional byte.
 * We reserve the header value 0x00 for NULL. Note that if reserved bits are 
 * present, the above header values will be shifted right logically by the 
 * number of reserved bits.
 */
public class IntWritableRowKey extends AbstractVarIntRowKey
{
  /** Header flags */
  protected static final byte INT_SIGN   = (byte) 0x80;
  protected static final byte INT_SINGLE = (byte) 0x40;
  protected static final byte INT_DOUBLE = (byte) 0x20;

  /** Header data bits for each header type */
  protected static final int INT_SINGLE_DATA_BITS = 0x6;
  protected static final int INT_DOUBLE_DATA_BITS = 0x5;
  protected static final int INT_EXT_DATA_BITS    = 0x3;

  /** Extended (3-9) byte length attributes */
  /** Number of bits in the length field */
  protected static final int INT_EXT_LENGTH_BITS = 0x2;

  public IntWritableRowKey() {
    super(INT_SINGLE, INT_SINGLE_DATA_BITS, INT_DOUBLE,
        INT_DOUBLE_DATA_BITS, INT_EXT_LENGTH_BITS, 
        INT_EXT_DATA_BITS);
  }

  @Override
  public Class<?> getSerializedClass() { return IntWritable.class; }

  @Override
  public Writable createWritable() { return new IntWritable(); }

  @Override
  public void setWritable(long x, Writable w) { 
    ((IntWritable)w).set((int)x); 
  }

  @Override
  public long getWritable(Writable w) { return ((IntWritable)w).get(); }

  @Override
  public long getSign(long l) { return l & Long.MIN_VALUE; }

  @Override
  protected byte initHeader(boolean sign) {
    return sign ? 0 : INT_SIGN; /* sign bit is negated in header */
  }

  @Override
  protected byte getSign(byte h) { 
    return (h & INT_SIGN) != 0 ? 0 : Byte.MIN_VALUE;
  }
}
