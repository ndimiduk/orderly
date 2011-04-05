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

/** Serialize and deserialize unsigned integers into a variable-length sortable
 * byte array representation. The basic idea is to treat these integers as 
 * signed integers with an implicit, zero-value sign bits and re-use the 
 * concepts from @{link IntWritableRowKey}. Specifically, we do not store any 
 * leading bits equal to the (implicit, zero-value) sign bit, so small values 
 * such as one fit in a single byte, while large values such as one billion use 
 * multiple bytes.
 * <p>
 * Our encoding consists of a header byte followed by 0-4 data bytes. The data
 * bytes are packed 8-bit data values in big-endian order. The header byte
 * contains the the number of data bytes and the 4-7 most significant
 * bits of data.
 * <p>
 * In the case of single byte encodings, the header byte contains 7 bits of
 * data. For double byte encodings, the header byte contains 6 bits of data, 
 * and for all other lengths the header byte contains 4 bits of data.
 * <p>
 * Thus we encode all numbers in two's complement using 
 * 2<sup>H+D</sup> data bits, where H is the number of data bits in the 
 * header byte and D is the number of data bits in the data bytes 
 * (D = number of data bytes &times; 8). 
 * <p>
 * More specifically, the numerical ranges for our variable-length byte 
 * encoding are:
 * <ul>
 *   <li> One byte: -128 &le; x &le; 128
 *   <li> Two bytes: -16384 &le; x &le; 16383
 *   <li> N &gt; 2 bytes: -2<sup>8 &times; (N-1) + 4</sup> &le; x 
 *        &le; 2<sup>8 &times; (N-1) + 4</sup> - 1
 * </ul>
 * We support all values that can be represented in a java int (treating
 * the sign bit as just another data bit), so N &le; 5.
 *
 * <h1> Reserved Bits </h1>
 *
 * Up to four of the most significant bits in the header may be reserved for 
 * use by the application, as four is the minimum number of data bits in the 
 * header byte. Reserved bits decrease the amount of data stored in the header 
 * byte. For example, a single byte encoding with two reserved bits can only 
 * encode integers in the range -32 &le; x &le; 31.
 *
 * <h1> Header Format </h1>
 * Given an integer x, the format of the header byte is 
 * <ul>
 *   <li>Bit 7: single-byte encoded ^ 1 (sign is always 0, negsign is always 1)
 *   <li>Bit 6: double-byte encoded ^ 1
 *   <li>Bit 4-5: len 
 * </ul>
 *
 * Bits 7 is used in all encodings. If bit 7 indicates a single byte
 * encoding, then bits 0-6 are all data bits. Otherwise, bit 6 is used to
 * indicate a double byte encoding. If this is true, then bits 0-5 are data
 * bits. Otherwise, bits 4-5 specify the length of the extended length (&gt; 2)
 * encoding, with a +3 bias as described below. In all cases, bits 0-3 are 
 * data bits.
 * <p>
 * The len field represents the length of the encoded byte array minus 3.
 * In other words, the encoded len field has a bias of +3, so an encoded
 * field with value 1 is 4 decoded.
 * <p>
 * Any trailing bits that are unused are padded with the sign bit of zero. The 
 * worst case overhead versus a standard fixed-length encoding is 1 additional 
 * byte.
 * <p>
 * We reserve the header value 0x00 for NULL. To ensure that non-NULL headers
 * do not produce a header equal to 0x00, we increment all header bytes by after
 * performing the above encoding operations. This will never result in 
 * arithmetic overflow because 0xFF is not a valid integer header byte.
 * Note that if reserved bits are present, the above 
 * header values will be shifted right logically by the number of reserved 
 * bits.
 */
public class UnsignedIntWritableRowKey extends AbstractVarIntRowKey
{
  /** Header flags */
  protected static final byte ULONG_SINGLE = (byte) 0x80;
  protected static final byte ULONG_DOUBLE = (byte) 0x40;

  /** Header data bits for each header type */
  protected static final int ULONG_SINGLE_DATA_BITS = 0x7;
  protected static final int ULONG_DOUBLE_DATA_BITS = 0x6;
  protected static final int ULONG_EXT_DATA_BITS    = 0x4;

  /** Extended (3-9) byte length attributes */
  /** Number of bits in the length field */
  protected static final int ULONG_EXT_LENGTH_BITS = 0x2; 

  public UnsignedIntWritableRowKey() {
    super(ULONG_SINGLE, ULONG_SINGLE_DATA_BITS, ULONG_DOUBLE,
        ULONG_DOUBLE_DATA_BITS, ULONG_EXT_LENGTH_BITS, 
        ULONG_EXT_DATA_BITS);
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
  public long getWritable(Writable w) { 
    int i = ((IntWritable)w).get();
    return ((long)i) & 0xffffffffL;
  }

  @Override
  public long getSign(long l) { return 0; }

  @Override
  protected byte initHeader(boolean sign) { return 0; }

  @Override
  protected byte getSign(byte h) { return 0; }

  @Override
  protected byte serializeNonNullHeader(byte b) { return (byte) (b + 1); }

  @Override
  protected byte deserializeNonNullHeader(byte b) { return (byte) (b - 1); }
}
