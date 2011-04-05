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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Serialize and deserialize unsigned long integers into a variable-length
 * sortable byte array representation. The basic idea is to only encode those 
 * bits that have values differing from the (implicit, zero-value) sign bit. 
 * <p>
 * Our encoding consists of a header byte followed by 0-8 data bytes. The data
 * bytes are packed 8-bit data values in big-endian order. The header byte
 * contains the the number of data bytes and the 3-7 most significant
 * bits of data.
 * <p>
 * In the case of single byte encodings, the header byte contains 7 bits of
 * data. For double byte encodings, the header byte contains 6 bits of data, 
 * and for all other lengths the header byte contains 3 bits of data.
 * <p>
 * Thus we encode all numbers using the 2<sup>H+D</sup> data bits, where H is 
 * the number of data bits in the header byte and D is the number of data bits 
 * in the data bytes (D = number of data bytes &times; 8). 
 * <p>
 * More specifically, the numerical ranges for our variable-length byte 
 * encoding are:
 * <ul>
 *   <li> One byte: -128 &le; x &le; 128
 *   <li> Two bytes: -16384 &le; x &le; 16383
 *   <li> N &gt; 2 bytes: -2<sup>8 &times; (N-1) + 3</sup> &le; x 
 *        &le; 2<sup>8 &times; (N-1) + 3</sup> - 1
 * </ul>
 * We support all values that can be represented in a java Long (treating the
 * sign bit as just another data bit), so N &le; 9.
 *
 * <h1> Reserved Bits </h1>
 *
 * Up to three of the most significant bits in the header may be reserved for 
 * use by the application, as three is the minimum number of data bits in the 
 * header byte. Reserved bits decrease the amount of data stored in the header 
 * byte. For example, a single byte encoding with two reserved bits can only 
 * encode integers in the range -32 &le; x &le; 31.
 *
 * <h1> Header Format </h1>
 * Given an integer, x, the full format of the header byte is 
 * <ul>
 *   <li>Bit 6: NOT single-byte encoded 
 *   <li>Bit 5: NOT double-byte encoded 
 *   <li>Bit 3-5: len 
 * </ul>
 *
 * Bits 7 is used in all encodings. If bit 7 indicates a single byte
 * encoding, then bits 0-6 are all data bits. Otherwise, bit 6 is used to
 * indicate a double byte encoding. If double byte encoding is specified, then 
 * bits 0-5 are data bits. Otherwise, bits 3-5 specify the length of the 
 * extended length (&gt; 2 byte) encoding, with a +3 bias as described below. 
 * In all cases, bits 0-2 are data bits.
 * <p>
 * The len field represents the length of the encoded byte array minus 3.
 * In other words, the encoded len field has a bias of +3, so an encoded
 * field with value 1 represents a length of 4 when decoded.
 * <p>
 * Any trailing bits that are unused are padded with the sign bit of zero. The 
 * worst case space overhead of this serialization format versus a standard 
 * fixed-length encoding is 1 additional byte.
 * <p>
 * All header bytes are incremented by one after applying the above operations.
 * This is done because the value 0x00 is reserved for NULL. An increment by one 
 * guarantees that NULL will be less than any valid integer header because 
 * 0xFF is a not a valid integer header and thus the increment will not result 
 * in arithmetic overflow.
 * <p>
 * Note that if reserved bits are present, the above 
 * header values will be shifted right logically by the number of reserved 
 * bits.
 */
public class UnsignedLongWritableRowKey extends AbstractVarIntRowKey
{
  /** Header flags */
  protected static final byte ULONG_SINGLE = (byte) 0x80;
  protected static final byte ULONG_DOUBLE = (byte) 0x40;

  /** Header data bits for each header type */
  protected static final int ULONG_SINGLE_DATA_BITS = 0x7;
  protected static final int ULONG_DOUBLE_DATA_BITS = 0x6;
  protected static final int ULONG_EXT_DATA_BITS    = 0x3;

  /** Extended (3-9) byte length attributes */
  /** Number of bits in the length field */
  protected static final int ULONG_EXT_LENGTH_BITS = 0x3; 

  public UnsignedLongWritableRowKey() {
    super(ULONG_SINGLE, ULONG_SINGLE_DATA_BITS, ULONG_DOUBLE,
        ULONG_DOUBLE_DATA_BITS, ULONG_EXT_LENGTH_BITS, 
        ULONG_EXT_DATA_BITS);
  }

  @Override
  public Class<?> getSerializedClass() { return LongWritable.class; }

  @Override
  public Writable createWritable() { return new LongWritable(); }

  @Override
  public void setWritable(long x, Writable w) { ((LongWritable)w).set(x); }

  @Override
  public long getWritable(Writable w) { return ((LongWritable)w).get(); }

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
