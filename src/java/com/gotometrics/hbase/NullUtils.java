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

package com.gotometrics.util;

import com.gotometrics.format.DataFormat;
import com.gotometrics.format.Order;
import com.gotometrics.util.ByteUtils;

import java.io.ByteArrayOutputStream;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Static utility functions for encoding and decoding byte arrays into
 * NULL-terminated form. The encoding format preserves HBase sort order, and 
 * supports both ascending and descending orderings. 
 *
 * This scheme NULL terminates variable-length byte arrays and ensures that
 * the NULL terminator compare less than (or if descending sort order, greater 
 * than) any possible non-NULL encoded value. Thus, if one value is the prefix 
 * of another (in the previous example "a" is a prefix of "aaa"), the NULL 
 * terminator of the shorter value will be compared to a non-NULL valid byte of 
 * the longer value. As the encoding guarantees that all encoded bytes have a
 * larger (smaller) value, the comparison will correctly sort the strings, 
 * breaking ties by length in the specified direction.
 *
 * <h1> Null Encoding </h1>
 *
 * The goal of null encoding is (1) to preserve sort order when compared
 * using HBase's byte array comparator so that for ascending sort orders
 * encoding(NULL) &lt; encoding(0) &lt; encoding(1) &lt; ... &lt; encoding(255),
 * and for descending sort orders encoding(0) &lt; encoding(1) &lt; ... &lt; 
 * encoding(255) &lt; encoding(NULL) (2) ensure that <b> all </b> encoded
 * values are non-zero (or for descending sort order not equal to 0xff) so that 
 * the variable-length values can be reliably NULL-terminated. 
 *
 * A pseudocode description of how a single byte is encoded is provided below:
 *
 *  function ascendingNullEncode(byte b) returns byte[] 
 *  {
 *     if (b in 0x00...0xfd)
 *       return { b + 1 }
 *     else
 *       return { 0xff, b + 3 } 
 *   }
 *
 *   function descendingNullEncode(byte b) returns byte[]
 *   {
 *     if (b in 0x02 ... 0xff)
 *       return { b - 1 }
 *     else
 *       return { 0x00, b + 1 } 
 *   }
 *
 *   Ascending byte arrays are terminated with a NULL byte value of 0x0, while
 *   descending byte arrays are terminated with a NULL byte value of 0xff.
 */

public class NullUtils
{
  private static final byte ASCENDING_TERMINATOR = (byte) 0x00,
                            ASCENDING_CONTINUATOR = (byte) 0xff,
                            ASCENDING_BIAS = (byte) 0x03;
  private static final byte DESCENDING_TERMINATOR = (byte) 0xff,
                            DESCENDING_CONTINUATOR = (byte) 0x00,
                            DESCENDING_BIAS = (byte) 0x01;


  private static byte getTerminator(Order ord) {
    return ord == Order.ASCENDING ? ASCENDING_TERMINATOR :
      DESCENDING_TERMINATOR;
  }

  private static byte getContinuator(Order ord) {
    return ord == Order.ASCENDING ? ASCENDING_CONTINUATOR : 
      DESCENDING_CONTINUATOR;
  }

  private static byte getDirection(Order ord) {
    return ord == Order.ASCENDING ? (byte)1 : (byte)-1;
  }

  private static byte getContinuationBias(Order ord) { 
    return ord == Order.ASCENDING ? ASCENDING_BIAS : DESCENDING_BIAS;
  }
  
  private static boolean isContinuedByte(Order ord, byte b) {
    byte continuator = getContinuator(ord);
    return b == continuator || b == continuator + -getDirection(ord);
  }

  /** Returns the length of a byte array if it was null encoded. Equal to
   * bytes.length + (number of continued byte values) + 1
   */
  public static int toEncodedLength(Order ord, ImmutableBytesWritable ibw) {
    byte[] b = ibw.get();
    int extraBytes = 0,
        offset = ibw.getOffset(),
        len = ibw.getLength();

    for (int i = offset; i < len; i++)
      if (isContinuedByte(ord, b[i]))
        extraBytes++;
    return len + extraBytes + 1;
  }


  /** Append the null encoding of src to dst.  */
  public static void encode(Order ord, ImmutableBytesWritable src, 
      ImmutableBytesWritable dst)
  {
    byte continuator = getContinuator(ord),
         dir = getDirection(ord),
         bias = getContinuationBias(ord);

    int srcOffset = src.getOffset(),
        dstOffset = dst.getOffset(),
        srcLen = src.getLength();
    byte[] dstBytes = dst.get();
    byte[] srcBytes = src.get();

    for (int i = 0; i < srcLen; i++, dstOffset++) {
      byte b = srcBytes[srcOffset + i];
      if (!isContinuedByte(ord, b)) {
        dstBytes[dstOffset] = (byte) (b + dir);
        continue;
      }
      /* Else we have to use a continuator byte */
      dstBytes[dstOffset] = continuator;
      dstBytes[++dstOffset] = (byte) (b + bias); /* evaluates to 1 or 2 */
    }
    dstBytes[dstOffset++] = getTerminator(ord);

    int len = dstOffset - dst.getOffset();
    dst.set(dstBytes, dstOffset, dst.getLength() - len);
  }

  public static byte[] encode(Order ord, byte[] srcBytes) {
    ImmutableBytesWritable src = new ImmutableBytesWritable(srcBytes);
    byte[] dstBytes = new byte[toEncodedLength(ord, src)];
    encode(ord, src, new ImmutableBytesWritable(dstBytes));
    return dstBytes;
  }

  /** Decode the null-encoded src and store the result in dst. Reads
   * up until a NULL terminator is discovered. If src == dst then a 
   * destructive update occurs, although the original byte[] backing src will 
   * remain unmodified.
   */
  public static void decode(Order ord, ImmutableBytesWritable src, 
      ImmutableBytesWritable dst) 
  {
    byte terminator = getTerminator(ord),
         continuator = getContinuator(ord),
         dir = getDirection(ord),
         bias = getContinuationBias(ord);

    byte[] srcBytes = src.get();
    int srcOffset = src.getOffset();

    byte[] dstBytes = dst.get();
    int dstOffset = dst.getOffset();

    for (int i = srcOffset; srcBytes[i] != terminator; i++) {
      byte b = srcBytes[i];
      if (b != continuator) {
        dstBytes[dstOffset++] = (byte) (b - dir);
      } else {
        dstBytes[dstOffset++] = (byte) (srcBytes[++i] - bias);
      }
    }

    int len = dstOffset - dst.getOffset();
    dst.set(dstBytes, dstOffset, dst.getLength() - len);
  }

  public static byte[] decode(Order ord, byte[] srcBytes) {
    ImmutableBytesWritable src = new ImmutableBytesWritable(srcBytes);
    byte[] dstBytes = new byte[toDecodedLength(ord, src)];
    decode(ord, src, new ImmutableBytesWritable(dstBytes));
    return dstBytes;
  }

  public static int toDecodedLength(Order ord, ImmutableBytesWritable ibw) {
    byte terminator = getTerminator(ord),
         continuator = getContinuator(ord);
    byte[] b = ibw.get();
    int offset = ibw.getOffset(),
        len = 0;

    for (int i = offset; b[i] != terminator; i++) 
      if (b[i] != continuator)
        len++;

    return len;
  }

  public static int length(Order ord, ImmutableBytesWritable ibw) {
    int offset = ibw.getOffset();
    byte[] b = ibw.get();
    byte terminator = getTerminator(ord);

    while (b[offset++] != terminator) ;
    return offset - ibw.getOffset();
  }

  public static void skipEncodedValue(Order ord, ImmutableBytesWritable ibw) {
    int len = length(ord, ibw);
    ibw.set(ibw.get(), ibw.getOffset() + len, ibw.getLength() - len);
  }
}
