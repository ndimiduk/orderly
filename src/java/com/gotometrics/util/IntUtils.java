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

package com.gotometrics.hbase.util;

import com.gotometrics.hbase.format.Order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** Utility class for converting to/from the GTM Variable Long Integer format.
 * The variable-length long format is a big-endian byte array
 * designed to succinctly represent small absolute values (i.e. -2 or 4) 
 * as these values are the most freqently encountered.
 *
 * Furthermore, this format preserves sort order when using a byte array
 * comparator, as is done in HBase. This requirement is what causes our design
 * to differ significantly from the conventional variable-length integer
 * encodings, such as Base-128 or Zig-Zag encodings. We also have a slightly 
 * denser encoding.
 *
 * The basic idea is to only encode those bits that have values differing
 * from the sign bit. Leading bits equal to the sign bit are implicit and
 * should not be specified. This means we have a compact representation for
 * values like -1 and +1, but require more bits for values such as 
 * +2^30 and -2^30.
 *
 * Our encoding consists of a header byte followed by 0-8 data bytes. The data
 * bytes are packed 8-bit data values in big-endian order. The header byte
 * contains the sign bit, the number of data bytes, and the 2-6 most significant
 * bits of data.
 *
 * In the case of single byte encodings, the header byte contains 6 bits of
 * data. For double byte encodings, the header byte contains 5 bits of data, 
 * and for all other lengths the header byte contains 2 bits of data.
 *
 * Thus we encode all numbers in two's complement using the sign bit in the 
 * header and 2^{H+D} data bits, where H is the number of data bits in the 
 * header byte and D is the number of data bits in the data bytes 
 * (D = number of data bytes * 8). 
 *
 * More specifically, the numerical ranges for our variable-length byte 
 * encoding are:
 * <ul>
 *   <li> One byte: -64 &le; x &le; 63
 *   <li> Two bytes: -8192 &le; x &le; 8191
 *   <li> N &gt; 2 bytes: -2<sup>8 * (N-1) + 2</sup> &le; x 
 *        &le; 2<sup>8 * (N-1) + 2</sup> - 1
 * </ul>
 * We support all values that can be represented in a java Long, so N &le; 9.
 *
 * <h1> Reserved Bits </h1>
 *
 * Up to two of the most significant bits in the header may be reserved for use
 * by the application, as two is the minimum number of data bits in the header
 * byte. Reserved
 * bits are typically used to embed variable-length integers within more complex
 * serialized data structures while preserving sort ordering. For example, the 
 * {@link BigDecimalUtils} class uses two reserved bits to efficiently embed a 
 * variable-length integer exponent within a serialized BigDecimal object.
 *
 * If R reserved bits are present, then the numerical range of the encoding is 
 * reduced by R bits, as the entire header byte is shifted right logically
 * R bits. The leading R bits are then initially set to zero and reserved 
 * entirely for use by the application, with the remaining least 
 * significant 8-R bits of the header byte used to store the header information
 * (and any data bits that will fit within the header byte). For example, a 
 * single byte encoding with two reserved bits can only encode integers in the 
 * range -16 &le; x &le; 15.
 *
 * <h1> Header Format </h1>
 * Given an integer, x: 
 *
 * sign = x &gt;&gt; Long.SIZE - 1
 * negSign = ~sign
 *
 * The format of the header byte is 
 * Bit 7: negSign
 * Bit 6: single-byte encoded ^ negSign
 * Bit 5: double-byte encoded ^ negSign
 * Bit 2-4: len ^ sign (each bit XOR'd with original, unnegated sign bit)
 *
 * Bits 6 and 7 are used in all encodings. If bit 6 indicates a single byte
 * encodng, then bits 0-5 are all data bits. Otherwise, bit 5 is used to
 * indicate a double byte encoding. If this is true, then bits 0-4 are data
 * bits. Otherwise, bits 2-4 specify the length of the multi-byte (&gt; 2)
 * encoding, with a +3 bias as described below. In all cases, bits 0-1 are 
 * data bits.
 *
 * The len field represents the length of the encoded byte array minus 3.
 * In other words, the encoded len field has a bias of +3, so an encoded
 * field with value 1 is 4 decoded.
 *
 * The XOR's with sign and negSign are required to preserve sort ordering when
 * using a big-endian byte array comparator to sort the encoded values.
 * 
 * Any trailing bits that are unused are padded with the sign bit. The worst 
 * case overhead versus a standard fixed-length encoding is 1 additional byte.
 *
 * Header values 0x00 and 0xff can never occur in a valid integer encoding. We 
 * reserve these values to represent NULL in a sort order-preserving single byte
 * encoding for ascending and descending sort orders, respectively. Note that
 * if reserved bits are present, these header values will be shifted right
 * appropriately by the number of reserved bits.
 */
public class IntUtils
{
  /* Header flags */
  private static final int HEADER_SIGN = 0x80;
  private static final int HEADER_SINGLE = 0x40;
  private static final int HEADER_DOUBLE = 0x20;

  /* Header data bits for each header type */
  private static final int HEADER_SINGLE_DATA_BITS = 0x6;
  private static final int HEADER_DOUBLE_DATA_BITS = 0x5;
  private static final int HEADER_MULTI_DATA_BITS = 0x2;

  /* Multi (3-9) byte length attributes */

  /* A multi byte length field has a minimum length of 3 bytes. Thus we store
   * all lengths with a bias of 3 so that we can pack the length into 3 bits.
   */
  private static final int HEADER_LEN_BIAS = 0x3;

  /* Number of bits in the length field */
  private static final int HEADER_LEN_BITS = 0x3; 

  /* Bit offset of the length field in the header byte */
  private static final int HEADER_LEN_OFF = 0x2; 

  /* Unsigned values omit the leading sign bit */
  private static final int HEADER_UNSIGNED_SHIFT = 0x1;


  /* NULL - ascending sort */
  private static final byte HEADER_NULL_ASC = (byte)0x00;

  /* NULL - descending sort */
  private static final byte HEADER_NULL_DSC = (byte)0xff;

  private static int getHeaderDataBits(int base, boolean isSigned) {
    return base + (isSigned ? 0 : HEADER_UNSIGNED_SHIFT);
  }

  private static int getHeaderSingleDataBits(boolean isSigned) {
    return getHeaderDataBits(HEADER_SINGLE_DATA_BITS, isSigned);
  }

  private static int getHeaderDoubleDataBits(boolean isSigned) {
    return getHeaderDataBits(HEADER_DOUBLE_DATA_BITS, isSigned);
  }

  private static int getHeaderMultiDataBits(boolean isSigned) {
    return getHeaderDataBits(HEADER_MULTI_DATA_BITS, isSigned);
  }

  /** Read a byte from x.  Any out of bounds bits (those whose bit position is
   * at offset &ge; 63) will be set to the sign bit.
   */
  private static byte readByte(final long x, final int offset, final int mask,
      boolean isSigned)
  {
    if (offset >= Long.SIZE - 1) 
      return isSigned ? (byte) ((x >> Long.SIZE - 1) & mask) : (byte) 0;
    return (byte) ((isSigned ? x >> offset : x >>> offset) & mask);
  }

  /** Return the number of data bits in the header.  */
  private static int getNumHeaderDataBits(int numBytes, boolean isSigned) {
    if (numBytes == 1)
      return getHeaderSingleDataBits(isSigned);
    else if (numBytes == 2)
      return getHeaderDoubleDataBits(isSigned);
    return getHeaderMultiDataBits(isSigned);
  }

  /** Returns an initialized header byte with all data bits clear.  */
  private static byte getHeader(byte reservedBits, long negSign, int numBytes,
      boolean isSigned)
  {
    long b = negSign & HEADER_SIGN;
    if (numBytes == 1) {
      b |= (~negSign & HEADER_SINGLE);
    } else if (numBytes == 2) {
      b |= (negSign & HEADER_SINGLE) | (~negSign & HEADER_DOUBLE);
    } else {
      byte encodedLength = (byte) ((((numBytes - HEADER_LEN_BIAS) ^ ~negSign) & 
            ((1 << HEADER_LEN_BITS) -1)) << HEADER_LEN_OFF);
      b |= (negSign & (HEADER_SINGLE|HEADER_DOUBLE)) | encodedLength;
    }

    if (!isSigned) 
      b <<= 1;

    return (byte) (b >>> reservedBits);
  }

  /** Encode a variable length long into a given byte array */
  public static void writeVarLong(final byte reservedBits, 
      final Long x, final byte[] b, final int offset, Order order, 
      boolean isSigned)
  {
    if (x == null) {
      if (order == Order.ASCENDING)
        b[offset] = (byte) (HEADER_NULL_ASC >>> reservedBits);
      else
        b[offset] = (byte) (HEADER_NULL_DSC >>> reservedBits);
      return;
    }

    long negSign = isSigned ? ~x >> Long.SIZE - 1 : -1;
    int numBytes = getVarLongLength(reservedBits, x, isSigned),
        headerBits = getNumHeaderDataBits(numBytes, isSigned) - reservedBits,
        numBits = headerBits + 8 * (numBytes - 1);

    if (reservedBits > getHeaderMultiDataBits(isSigned))
      throw new IllegalArgumentException("Cannot reserve more than " +
          getHeaderMultiDataBits(isSigned)  + " bits");

    /* Encode the header and any header data bits */
    b[offset] = getHeader(reservedBits, negSign, numBytes, isSigned);
    b[offset] |= readByte(x, numBits -= headerBits, (1 << headerBits) - 1,
        isSigned);
    if (!isSigned) 
      b[offset]++;

    /* Now encode numBytes - 1 data bytes */
    for (int i = 1; i < numBytes; i++)  
      b[offset + i] = readByte(x, numBits -= 8, 0xff, isSigned); 
  }

  public static void writeVarLong(final byte reservedBits, 
      final Long x, final byte[] b, final int offset, Order order) 
  {
    writeVarLong(reservedBits, x, b, offset, order, true);
  }

  /** Encode a variable length long into a given byte array */
  public static void writeVarLong(final byte reservedBits, 
      final Long x, final byte[] b, final int offset)
  {
    writeVarLong(reservedBits, x, b, offset, Order.ASCENDING);
  }

  /** Encode a variable length long into a given byte array */
  public static void writeVarLong(final Long x, final byte[] b, 
      final int offset)
  {
    writeVarLong(x, b, offset, Order.ASCENDING);
  }

  public static void writeVarLong(final Long x, final byte[] b, 
      final int offset, boolean isSigned)
  {
    writeVarLong(x, b, offset, Order.ASCENDING, isSigned);
  }

  /** Encode a variable length long into a given byte array */
  public static void writeVarLong(final Long x, final byte[] b, 
      final int offset, Order order)
  {
    writeVarLong((byte)0, x, b, offset, order);
  }

  public static void writeVarLong(final Long x, final byte[] b, 
      final int offset, Order order, boolean isSigned)
  {
    writeVarLong((byte)0, x, b, offset, order, isSigned);
  }

  /** Encode a variable length long into the given DataOutput */
  public static void writeVarLong(final byte reservedBits, final Long x, 
      final DataOutput out) throws IOException
  {
    byte[] b = new byte[getVarLongLength(reservedBits, x)];
    writeVarLong(reservedBits, x, b, 0);
    out.write(b);
  }

  /** Encode a variable length long into the given DataOutput */
  public static void writeVarLong(final Long x, final DataOutput out) 
    throws IOException
  {
    writeVarLong((byte)0, x, out);
  }

  /** Encode a variable length integer into a given byte array */
  public static void writeVarInt(final byte reservedBits, final int x, 
      final byte[] b, final int offset) 
  {
    writeVarLong(reservedBits, Long.valueOf(x), b, offset);
  }

  /** Encode a variable length integer into a given byte array */
  public static void writeVarInt(final int x, final byte[] b, final int offset) 
  {
    writeVarInt((byte)0, x, b, offset);
  }

  /** Encode a variable length integer into the given DataOutput */
  public static void writeVarInt(final byte reservedBits, final int x, 
      final DataOutput out) throws IOException
  {
    writeVarLong(reservedBits, Long.valueOf(x), out);
  }

  /** Encode a variable length integer into the given DataOutput */
  public static void writeVarInt(final int x, final DataOutput out) 
    throws IOException
  {
    writeVarInt((byte)0, x, out);
  }

  public static boolean isNull(byte h, Order order, int reservedBits) {
    h = (byte) ((h << reservedBits) >> reservedBits);
    return (order == Order.ASCENDING && h == HEADER_NULL_ASC)
        || (order == Order.DESCENDING && h == HEADER_NULL_DSC);
  }

  /** Decode the length of a variable-length long from its header byte */
  public static int decodeVarLongLength(final byte reservedBits, final byte h,
      Order order, boolean isSigned)
  {
    byte b = (byte) (h << reservedBits),
         negSign = isSigned ? (byte) (b >> Byte.SIZE - 1) : (byte) -1;

    if (!isSigned) {
      b--;
      b >>>= 1;
    }

    if (isNull(h, order, reservedBits) || ((b ^ negSign) & HEADER_SINGLE) != 0)
      return 1;
    else if (((b ^ negSign) & HEADER_DOUBLE) != 0)
      return 2;

    int len = ((b ^ ~negSign) >>> HEADER_LEN_OFF) & ((1 << HEADER_LEN_BITS)-1);
    return len + HEADER_LEN_BIAS;
  }

  public static int decodeVarLongLength(final byte reservedBits, final byte h) {
    return decodeVarLongLength(reservedBits, h, Order.ASCENDING, true);
  }

  public static int decodeVarLongLength(final byte reservedBits, final byte h,
      boolean isSigned)
  {
    return decodeVarLongLength(reservedBits, h, Order.ASCENDING, isSigned);
  }

  /** Decode the length of a variable-length long from its header byte */
  public static int decodeVarLongLength(final byte b, Order order) {
    return decodeVarLongLength((byte)0, b, order, true);
  }

  /** Decode the length of a variable-length long from its header byte */
  public static int decodeVarLongLength(final byte b, Order order,
      boolean isSigned) 
  {
    return decodeVarLongLength((byte)0, b, order, isSigned);
  }

  public static int decodeVarLongLength(final byte b) {
    return decodeVarLongLength(b, Order.ASCENDING, true);
  }

  /** Decode the length of a variable-length integer from its header byte */
  public static int decodeVarIntLength(final byte reservedBits, final byte b) {
    return decodeVarLongLength(reservedBits, b);
  }

  /** Decode the length of a variable-length integer from its header byte */
  public static int decodeVarIntLength(final byte b) {
    return decodeVarLongLength(b);
  }

  /** Returns the number of bits required to represent x in a minimal-length
   * two's complement representation (excluding the sign bit).
   *
   * Compute bitsize(x) using number of leading zeroes. We use Long.SIZE 
   * rather than Long.SIZE + 1 as the minuend in the subtraction operation 
   * because we are calculating the number of bits required _excluding_ the 
   * sign bit
   *
   * Reference: Hacker's Delight, 5.3 "Relation to the Log Function", 
   *            bitsize(x)
   */
  private static int bitSize(final long x, boolean isSigned) {
    long diffBits = isSigned ? x ^ (x >> Long.SIZE - 1) : x;
    return Long.SIZE - Long.numberOfLeadingZeros(diffBits);
  }

  /** Return the number of bytes necessary to encode a long using 
   * variable-length long encoding.
   */
  public static int getVarLongLength(final byte reservedBits, final Long x,
      boolean isSigned)
  {
    if (x == null)
      return 1;

    int numBits = bitSize(x, isSigned) + reservedBits;
    

    /* Check to see if x can fit into the number of data bits in a single or
     * double byte encoding. We test against HEADER_DOUBLE_DATA_BITS + 8 because
     * a double byte encoding has data bits in its header and a trailing 8-bit
     * data byte.
     */
    if (numBits <= getHeaderSingleDataBits(isSigned))
      return 1;
    else if (numBits <= getHeaderDoubleDataBits(isSigned) + 8)
      return 2;

    /* Otherwise, x will require a multi (3-9) byte encoding. At this point we 
     * know one of the bits is not equal to the sign bit, as -1 and 0 are 
     * handled by the single byte special case. Therefore 
     * numBits < Long.SIZE. We want to encode bits 0 ... numBits-1 into as few
     * bytes as possible. We will need one header byte and 2+ data bytes.
     *
     * Per Hacker's Delight 3.1 -- rounding x up to the nearest multiple of 8 
     * and then dividing by 8 unsigned is ((x + 7) & -8) >>> 3. Thus we require
     * 1 + ((numBits - headerBits + 7) >>> 3) bytes. One byte stores the 
     * header (and headerBits of the data), and the remaining bytes store the
     * rest of the data in big endian order. The AND of -8 can be removed 
     * because the subsequent logical right shift will remove the least 
     * significant 3 bits anyway.
     */
    return 1 + ((numBits - getHeaderMultiDataBits(isSigned) + 7) >>> 3);
  }

  public static int getVarLongLength(final byte reservedBits, final Long x) {
    return getVarLongLength(reservedBits, x, true);
  }

  /** Return the number of bytes necessary to encode a long using 
   * variable-length long encoding.
   */
  public static int getVarLongLength(final Long x, boolean isSigned) {
    return getVarLongLength((byte)0, x, isSigned);
  }

  public static int getVarLongLength(final Long x) {
    return getVarLongLength(x, true);
  }

  /** Return the number of bytes necessary to encode an int using 
   * variable-length integer encoding.
   */
  public static int getVarIntLength(final byte reservedBits, final int x) {
    return getVarLongLength(reservedBits, Long.valueOf(x));
  }

  /** Return the number of bytes necessary to encode an int using 
   * variable-length integer encoding.
   */
  public static int getVarIntLength(final int x) {
    return getVarLongLength((byte)0, Long.valueOf(x));
  }

  /** Write byte b to long x. Any bits that will be written out of bounds
   * (offset &ge; 63) will be ignored. Assumes x is initialized to  
   * sign bit &gt;&gt; 63. */
  private static long writeByte(byte b, int offset, int mask, long x,
      boolean isSigned)
  {
    if (offset >= Long.SIZE - 1) 
      return x;

    /* We only encode bytes where a bit differs from the sign bit, so we OR
     * in 1 bits from byte b if x is positive (as its sign bit is 0), and mask 
     * out/clear using 0 bits from b if x is negative. The long casts are 
     * necessary for 64-bit shift offsets (see Java Language Spec. 15.19).
     */
    if (x >= 0 || !isSigned) 
      x |= (long)(b & mask) << offset;
    else 
      x &= ~((long)(~b & mask) << offset);
    return x;
  }

  /** Decode a variable-length long from a byte array */
  public static Long readVarLong(final byte reservedBits, final byte[] b, 
      final int offset, Order order, boolean isSigned) 
  {
    if (isNull(b[offset], order, reservedBits))
      return null;

    int numBytes = decodeVarLongLength(reservedBits, b[offset], isSigned),
        headerBits = getNumHeaderDataBits(numBytes, isSigned) - reservedBits,
        numBits = headerBits + 8 * (numBytes - 1);

    /* Sign extend and right-propagate the header sign bit */
    long signMask = HEADER_SIGN >>> reservedBits,
         negSign = isSigned ? -(b[offset] & signMask) >> Long.SIZE - 1 : -1,
         x = ~negSign;

    if (reservedBits > getHeaderMultiDataBits(isSigned))
      throw new IllegalArgumentException("Cannot reserve more than " +
          getHeaderMultiDataBits(isSigned) + " bits");
 
    x = writeByte((byte) (b[offset] - (isSigned ? 0 : 1)), 
        numBits -= headerBits, (1 << headerBits) - 1, x, isSigned);
    for (int i = 1; i < numBytes; i++) 
      x = writeByte(b[offset + i], numBits -= 8, 0xff, x, isSigned);
    return x;
  }

  public static Long readVarLong(final byte reservedBits, final byte[] b, 
      final int offset, Order order) {
    return readVarLong(reservedBits, b, offset, order, true);
  }

  public static Long readVarLong(final byte reservedBits, final byte[] b, 
      final int offset)
  {
    return readVarLong(reservedBits, b, offset, Order.ASCENDING);
  }

  public static Long readVarLong(final byte reservedBits, final byte[] b, 
      final int offset, boolean isSigned)
  {
    return readVarLong(reservedBits, b, offset, Order.ASCENDING, isSigned);
  }
  
  /** Decode a variable-length long from a byte array */
  public static Long readVarLong(final byte[] b, final int offset) {
    return readVarLong(b, offset, Order.ASCENDING);
  }

  public static Long readVarLong(final byte[] b, final int offset, 
      boolean isSigned) 
  {
    return readVarLong(b, offset, Order.ASCENDING, isSigned);
  }

  /** Decode a variable-length long from a byte array */
  public static Long readVarLong(final byte[] b, final int offset, Order order)
  {
    return readVarLong((byte)0, b, offset, order);
  }

  public static Long readVarLong(final byte[] b, final int offset, Order order,
      boolean isSigned)
  {
    return readVarLong((byte)0, b, offset, order, isSigned);
  }

  /** Decode a variable-length long from a DataInput object */
  public static Long readVarLong(final byte reservedBits, final DataInput in,
      boolean isSigned) throws IOException 
  {
    byte header = in.readByte();
    byte[] b = new byte[decodeVarLongLength(reservedBits, header, isSigned)];

    b[0] = header;
    in.readFully(b, 1, b.length - 1);
    return readVarLong(reservedBits, b, 0, isSigned);
  }

  public static Long readVarLong(final byte reservedBits, final DataInput in)
    throws IOException
  {
    return readVarLong(reservedBits, in, true);
  }

  /** Decode a variable-length long from a DataInput object */
  public static Long readVarLong(final DataInput in) throws IOException 
  {
    return readVarLong((byte)0, in);
  }

  /** Decode a variable-length integer from a byte array */
  public static int readVarInt(final byte reservedBits, final byte[] b, 
      final int offset) 
  {
    return (int) readVarLong(reservedBits, b, offset).longValue();
  }

  /** Decode a variable-length integer from a byte array */
  public static int readVarInt(final byte[] b, final int offset) {
    return (int) readVarLong((byte)0, b, offset).longValue();
  }

  /** Decode a variable-length integer from a DataInput object */
  public static int readVarInt(final byte reservedBits, final DataInput in) 
    throws IOException 
  {
    return (int) readVarLong(reservedBits, in).longValue();
  }

  /** Decode a variable-length integer from a DataInput object */
  public static int readVarInt(final DataInput in) throws IOException {
    return (int) readVarLong((byte)0, in).longValue();
  }
}
