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

import java.math.BigDecimal;
import java.math.BigInteger;

/** Utility class for converting to/from the GTM Variable BigDecimal format.
 * The variable-length BigDecimal format is a big-endian byte array
 * designed to succinctly represent small absolute values (i.e. -2e1 or 4.1e-8),
 * while preserving sort order.
 *
 * A {@link BigDecimal} object is composed of a power of 10 exponent scale and
 * an unscaled, arbitrary precision integer significand. The value of the 
 * BigDecimal is unscaled base 2 significand * 10<sup>scale</sup>. The 
 * significand is an unscaled, arbitrary precision {@link BigInteger}, while 
 * the scale is a signed 32-bit int.
 *
 * This encoding format converts a canonicalized BigDecimal into an unscaled,
 * arbitrary precision base-10 integer significand and a power-of-10 adjusted 
 * exponent. As described in {@link BigDecimal.toString}, an adjusted exponent 
 * is equal to the scale + precision - 1, where precision is the number of 
 * digits in the unscaled base 10 significand. 
 *
 * The base-10 exponent and significand are then translated
 * into a sequence of bytes in such a way that BigDecimal sort order is
 * preserved. We encode the BigDecimal as a (base 10 exponent,
 * base 10 significand) pair to preserve sort ordering. This process is 
 * described in detail below.
 *
 * <h1> Canonicalization </h1>
 *
 * All BigDecimal values first go through canonicalization by stripping any
 * trailing zeros using the {@link BigDecimal.stripTrailingZeros} method.
 * This avoids ambiguous but numerically equivalent byte representations and
 * also ensures that no space is wasted storing redundant trailing zeros.
 *
 * <h1> Normalization </h1>
 *
 * Next, the unscaled value and the scale must be normalized to a common base.
 * This is necessary to preserve sort order. To see why, note that we when we
 * compare two BigDecimal objects, we may need to compare their significands,
 * most significant byte first. However, for this comparison to be accurate we
 * we must know the exponent of the significand digits. For example, the value
 * of the first digit may be 4, but in order to compare this digit to the first
 * digit in another BigDecimal we must know if the first digit is multipled by
 * an exponent of 10<sup>4</sup> or 10<sup>-200</sup>. The significand digits
 * should only be compared if the exponents match.
 *
 * Associating an exponent with a significand digit requires both the unscaled 
 * significand and the power of 10 exponent to have a common 
 * base. The {@link BigInteger} unscaled value is encoded in base 2, 
 * while the scale is a power of ten exponent, and represents 
 * 10<sup>scale</sup>. 
 *
 * We can either convert the scale to a power of 2 exponent, or convert the 
 * unscaled value to base 10. The former option is not acceptable because 
 * an integer power of ten does not always convert to an integer power of two.
 * In fact, many negative powers of ten, such as 10<sup>-1</sup> have an
 * <i>infinite</i> repeating representation when encoded in binary.
 *
 * Fortunately, the unscaled value is an arbitrary precision integer and 
 * contains no fractional component. Non-fractional integers can be converted 
 * from one integer base to another without loss of precision and without any 
 * of the infinite repeating digit problems that can be encountered when 
 * converting fractional values.
 *
 * We convert the BigInteger to its decimal String representation using 
 * {@link BigInteger.toString(10)}. We take the absolute value by 
 * removing the leading '-' if the unscalued value is negative, and encode
 * the resulting decimal String into bytes using the Binary Coded Decimal format
 * described below. The sign bit of the unscaled value is encoded into the
 * header byte, as described in the Header section.
 *
 * <h1> Null Terminated Binary Coded Decimals </h1>
 *
 * We convert decimal Strings into Binary Coded Decimal by mapping 
 * the ASCII characters '0' ... '9' to binary 1 ... 10. Each ASCII digit
 * is encoded into a 4-bit nibble. There are two nibbles per byte. A nibble of 0
 * is used as the null terminator to indicate the end of the BCD encoded string.
 * This ensures that shorter strings that are the prefix of a longer string will
 * always compare less than the longer string, as the null terminator is a 
 * smaller value that any decimal digit.
 *
 * The BCD encoding thus requires an extra byte of space only if 
 * there are an even number of characters. An odd-length string does not use
 * the least significant nibble of its last byte, and thus can include the null 
 * terminator nibble without requiring any additional bytes.
 *
 * <h1> Exponent </h1>
 *
 * The adjusted exponent is equal to scale + precision - 1, where precision is
 * equal to the number of the digits in the base 10 unscaled significand. We
 * translate the adjusted exponent into bytes using 
 * {@link IntUtils.writeVarLong}. A long encoding is chosen to avoid 
 * overflow because scale and precision are both 32-bit integers and thus their
 * sum requires 33-bits in the worst case.
 *
 * <h1> Ordering </h1>
 *
 * The encodings described so far are sufficient to handle positive BigDecimals 
 * sorted in ascending order. However, we must also support negative 
 * BigDecimals, as well as descending sort orderings. The special case of a 
 * BigDecimal zero is discussed in the Header section. 
 *
 * Fortunately, this can be easily accomplish by realizing that -x &lt; -y if
 * and only if y &gt; x. As both signed and unsigned comparison operations are
 * bitwise operations, we can reverse the sort order of a signed or unsigned
 * byte b  by taking its logical inverse (1's complement), ~b.
 *
 * We refer to the <i> order </i> of a BigDecimal as its significand sign XOR
 * its order direction, where ascending order has a direction of 0 and
 * descending order has a direction of -1. The order byte is either 
 * all zeros (0) or all ones (-1). If the order byte is all ones, then XOR'ing
 * a byte b with the order byte will take the logical complement of b. If the
 * order byte is all zeros, then XOR'ing a byte b with the order byte is
 * equivalent to the identity operation and does not change the value of b.
 *
 * Each byte in the BCD encoding of the unscaled significand is XOR'd with the 
 * order byte
 * before being stored into a byte array. Similarly, the sign extended order 
 * byte is XOR'd with the adjusted exponent before it is converted to a byte
 * array by {@link IntUtils.writeVarLong}. This ensures that we reverse the sort
 * ordering of the adjusted exponent and the unscaled significand if the 
 * situation requires it (e.g., when encoding positive numbers with descending
 * sort order or negative numbers with ascending sort order). 
 *
 * This technique only ensures
 * correct sort ordering between two values with the same sign. We rely on the
 * header byte to ensure that comparisons involving two values with different
 * signs work correctly (i.e. that negative numbers sort less than zero or any
 * positive number).
 *
 * <h1> Header </h1>
 *
 * The header byte serves two purposes: to encode the <i> order </i> 
 * (sign bit of the significand XOR direction of the sort ordering) so that we
 * have correct sort ordering between values with different signs, and
 * to encode BigDecimal values of zero.
 *
 * The adjusted exponent cannot directly represent zero. There is no power of 10
 * exponent that results in zero, as zero is effectively 10<sup>-infinity</sup>.
 * However, we don't want to encode zero as a magic small value, such as a 
 * Long.MIN_VALUE as this would require more than one byte to represent given
 * that adjusted exponents are encoded using {@link IntUtils.writeVarLong}, and
 * zero is a relatively common value.
 *
 * To resolve these issues, our header consists of two bits: the negated order
 * bit, and the isZero bit.  We properly sort values of different signs by 
 * making the most 
 * significant bit of the header be the logical inverse (negation) of the order.
 * For ascending order, this ensures that positive values sort greater than all
 * negative values. For descending byte order, this ensures the opposite.
 *
 * The isZero bit is logically true if the BigDecimal is equal to zero (i.e. if
 * BigDecimal.unscaledValue().equals(BigInteger.ZERO)). However, to ensure
 * appopriate sort ordering for ascending and descending sorts, the zero bit
 * is XOR'd with the negated order bit before it is stored in the header.
 *
 * We would waste 6 bits if we encoded our two header bits into a byte. For
 * performance and efficiency reasons, we instead include the header bits 
 * as the most significant bits of the variable-length long header representing
 * the adjusted exponent. This relies on the 'reserved bits' feature of 
 * {@link VarLongUtils.writeVarLong}, which allows application to reserve up to
 * the two most significant bits of the first byte of a variable-length long.
 * This applies only to non-zero BigDecimals -- BigDecimals that are zero-valued
 * have no associated exponent and are always stored in a single byte consisting
 * solely of the header bits.
 *
 * <h1> Format Summary </h1>
 * A non-zero encoded BigDecimal consists of an adjusted exponent with 
 * our header bits and an unscaled base-10 significand. A BigDecimal that
 * is equal to zero consists of a single byte containing only our two header 
 * bits.
 * 
 * For non-zero BigDecimal values, the exponent is XOR'd with the sign-extended
 * order byte and encoded via {@link IntUtils.writeVarLong}, which outputs an 
 * IntUtils header byte and 0-4 data bytes. 
 *
 * We reserve the most significant two bits of the first byte (the IntUtils 
 * header byte) to store our two header bits, the negated order bit and
 * the isZero bit. The negated order is stored in the most significant bit of 
 * the header byte, and is calculated by taking the logical negation of the 
 * order and comparing the result to zero. The isZero bit is logically true if 
 * the BigDecimal's unscaled significand is equal to zero, but the bit is XOR'd
 * with the negated order bit before being stored into the header. 
 *
 * In non-zero BigDecimals, the significand follows the exponent consists of
 * one or more packed BCD bytes encoding the decimal representation of the 
 * base-10 significand in most significant byte (and nibble) order. 
 * The packed BCD string is terminated by a null nibble.
 */

public class BigDecimalUtils
{
  private static final int HEADER_NEGORDER = 0x80;
  private static final int HEADER_SIGNIFICAND_ZERO = 0x40;
  private static final byte RESERVED_BITS = 0x2;

  private static byte getSign(Order ord) {
    return (byte) (ord == Order.ASCENDING ? 0 : -1);
  }

  /** Returns the length of a String s if encoded in BCD format. We require
   * 1 byte for every 2 characters, rounding up. Furthermore, if the number
   * of characters is even, we require an additional byte for the null
   * terminator.
   */
  private static int getBCDEncodedLength(String s) {
    return (s.length() + 2) >>> 1;
  }

  /** Convert a decimal String s into packed, null-terminated BCD format */
  private static void toBCD(byte order, String s, byte[] b, int offset) {
    int strLength = s.length(),
        bcdLength = getBCDEncodedLength(s);

    for (int i = 0, pos = 0; i < bcdLength; i++, pos++) {
      byte bcd = 0;
      if (pos < strLength)
        bcd = (byte) (1 + Character.digit(s.charAt(pos), 10) << 4);
      if (++pos < strLength)
        bcd |= (byte) (1 + Character.digit(s.charAt(pos), 10));
      b[offset + i] = (byte) (bcd ^ order);
    }
  }

  /** Convert a BigDecimal into a sequence of bytes, preserving sort order */
  public static byte[] toBytes(Order ord, BigDecimal d) {
    byte[] b;

    if (d == null) {
      b = new byte[1];
      IntUtils.writeVarLong(RESERVED_BITS, null, b, 0, ord);
      b[0] = (byte) ((b[0] << RESERVED_BITS) >> RESERVED_BITS);
      return b;
    }

    d = d.stripTrailingZeros();
    BigInteger i = d.unscaledValue();
    /* Bit trick - i.signum() >>Integer.SIZE-1 is -1 if i < 0, 0 if i >= 0 */
    byte order = (byte) (getSign(ord) ^ (i.signum() >> (Integer.SIZE - 1))),
         header = (byte) (~order & HEADER_NEGORDER);
    int scale = -d.scale();

    if (i.signum() == 0) 
      return new byte[] { (byte) (header | (order & HEADER_SIGNIFICAND_ZERO)) };
    else
      header |= (byte) (~order & HEADER_SIGNIFICAND_ZERO);

    String s = i.toString(10);
    if (i.signum() < 0)
      s = s.substring(1); /* Skip leading '-' */

    long precision = s.length(),
         exp = precision + scale -1L;
    int expLen = IntUtils.getVarLongLength(RESERVED_BITS, exp ^ order);

    b = new byte[expLen + getBCDEncodedLength(s)];
    IntUtils.writeVarLong(RESERVED_BITS, exp ^ order, b, 0);
    b[0] |= header;
    toBCD(order, s, b, expLen);
    return b;
  }

  /** Convert a packed, null-terminated BCD array into a decimal String */
  private static String toString(byte order, byte[] b, int offset) {
    int i = 0,
        shift = 4;
    StringBuilder sb = new StringBuilder();

    while(true) {
      byte bcd = (byte) (((b[offset + (i >>> 1)] ^ order) >>> shift) & 0xf);
      if (bcd == 0)
        break;
      sb.append((char) ('0' + bcd - 1));
      i++;
      shift ^= 4;
    }

    return sb.toString();
  }

  /** Convert an encoded sequence of bytes into a BigDecimal object */
  public static BigDecimal toBigDecimal(Order ord, byte[] b, int offset) {
    byte order = (byte) (~b[offset] >> Byte.SIZE - 1);

    if (IntUtils.isNull(b[offset], ord, RESERVED_BITS))
      return null;
    else if (((b[offset] ^ ~order) & HEADER_SIGNIFICAND_ZERO) != 0)
      return BigDecimal.ZERO;

    long exp = IntUtils.readVarLong(RESERVED_BITS, b, offset) ^ order;
    int expLen = IntUtils.decodeVarLongLength(RESERVED_BITS, b[offset]);

    String s = toString(order, b, offset + expLen);
    int precision = s.length(),
        scale = (int) (exp - precision + 1L); 

    if ((order ^ getSign(ord)) != 0) /* Extract significand sign bit */
      s = '-' + s;
    BigInteger i = new BigInteger(s);
    return new BigDecimal(i, -scale);
  }

  /** Convert an encoded sequence of bytes into a BigDecimal object */
  public static BigDecimal toBigDecimal(Order ord, byte[] b) {
    return toBigDecimal(ord, b, 0);
  }
}
