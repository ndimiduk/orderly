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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serialize and deserialize UTF-8 byte arrays into HBase row keys.
 * Most of the work is done for us because sorting byte arrays of UTF-8 encoded 
 * characters is equivalent to sorting the equivalent decoded Unicode strings by
 * Unicode code point. This is discussed further in the 
 * <a href="http://en.wikipedia.org/wiki/UTF-8"> UTF-8 Wikipedia article</a>.
 * As a historical aside, this nifty and very useful property of UTF-8 is due to
 * Ken Thompson and Rob Pike of Unix fame. UTF-8 has many other awesome 
 * properties, like being fully self-synchronized.
 *
 * Unfortunately, we cannot just use raw UTF-8 bytes -- we also need to 
 * encode NULL, as well as a string terminator to indicate the end of the 
 * string. When sorting, we must ensure that NULL &lt; terminator &lt; any valid
 * UTF-8 byte so that strings sort in the correct order. Fortunately, a simple 
 * solution is available to us for encoding NULL and terminator bytes. UTF-8 
 * encoding will never produce the byte values 0xff or 0xfe. Thus, we may reserve 
 * <b>0x00</b> for NULL and <b>0x01</b> for terminator if we add 2 to each 
 * UTF-8 byte when encoding the UTF-8 byte array. 
 *
 * To encode a NULL, we output 0x0 and return. Otherwise, to encode a non-NULL
 * UTF-8 byte array we add 2 to each of the raw utf-8 bytes and then append the
 * terminator byte at the end. Decoding is simply the reverse of the above 
 * operations.
 *
 * <h1> Descending sort </h1>
 * To sort in descending order we perform the same encodings as in ascending 
 * sort, except we logically invert (take the 1's complement) each byte, including
 * the null and termination bytes. Descending sort cannot use implicit 
 * termination (discussed below).
 *
 * <h1> Implicit termination </h1>
 * There is a single case where we deviate from the above encoding format.
 * If this class is the last class to serialize a row key byte, then we can
 * use the end of the row key as an implicit terminator if we are performing
 * an ascending sort. This avoids serializing the termination byte entirely.  We 
 * cannot use implicit termination with descending sort because in descending 
 * sort, &quot;aaa&quot; &lt; &quot;aa&quot;, but when using implicit 
 * termination &quot;aaa&quot; &gt; &quot;aa&quot; because there is no 
 * terminator byte to invert.
 *
 * To perform implict termination for a non-empty, non-NULL string, we 
 * speciy that we reach the end of the UTF-8 string when we reach the end of 
 * the underlying byte array array. This allows us to avoid using a terminator 
 * byte for non-empty, non-NULL strings.
 *
 * In this encoding format, NULL bytes are be represented by a zero-byte array,
 * and empty strings consist of a single terminator byte. We encode non-empty 
 * strings by adding 2 to each UTF-8 byte as before, but we now omit the 
 * trailing terminator byte. For ascending sort, this byte representation still 
 * guarantees that NULL &lt; empty string &lt; non-empty string. This works
 * because NULL, when encoded as a zero-byte array, compares less than
 * any non-zero array of bytes. 
 *
 *
 * <h1> Usage </h1>
 * This is the fastest class for storing characters and strings. Two copies are
 * It performs the minimum amount of copying during serialization and 
 * deserialization, performing only a single copy each time.
 */
public class UTF8Key extends RowKey 
{
  private static final byte NULL = (byte)0x00;
                           TERMINATOR = (byte)0x01;

  @Override
  public Class<?> getSerializedClass() { return byte[].class; }

  protected boolean mustTerminate() {
    return !order.equals(Order.ASC) || !isLastKey;
  }

  protected boolean isNull(ImmutableBytesWritable w) {
    byte[] b = w.get();
    int offset = w.getOffset();
    return !mustTerminate() ? b.getLength() == 0 : mask(b[offset]) == NULL;
  }

  protected boolean isEmpty(ImmutableBytesWritable w) {
    byte[] b = w.get();
    int offset = w.getOffset();
    return mask(b[offset]) == TERMINATOR;
  }

  @Override
  public long getSerializedLength(Object o) throws IOException {
    if (o == null)
      return mustTerminate() ? 1 : 0;
    return Math.max(((byte[])o).length + (mustTerminate() ? 1 : 0), 1);
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) 
    throws IOException
  {
    byte[] b = w.get();
    int offset = w.getOffset();

    if (o == null) {
      if (mustTerminate())
        b[offset] = mask(NULL);
      return;
    }

    byte[] s = (byte[]) o;
    int len = s.length;

    for (int i = 0; i < s.length; i++)
      b[offset + i] = mask((byte)(s[i] + 2));
    if (mustTerminate() || s.length == 0)
      b[offset + len++] = mask(TERMINATOR);
    w.set(w.get(), w.getOffset() + len, w.getLength() - len);
  }

  protected int getUTF8Length(ImmutableBytesWritable w) {
    byte[] b = w.get();
    int offset = w.getOffset();

    if (isNull(w) || isEmpty(w)) 
      return 0;
    else if (!mustTerminate()) 
      return w.getLength();

    while (mask(b[offset]) != TERMINATOR)
      offset++;
    return offset - w.getOffset();
  }

  @Override
  public void skip(ImmutableBytesWritable w) throws IOException {
    int len = getUTF8Length(w) + (mustTerminate() ? 1 : 0);
    w.set(w.get(), w.getOffset() + len, w.getLength() - len);
  }

  @Override
  public byte[] deserialize(ImmutableBytesWritable w) throws IOException {
    if (isNull(w)) 
      return null;
    else if (isEmpty(w))
      return ByteUtils.EMPTY;

    int len = getUTF8Length(w),
        offset = w.getOffset();
    byte[] s = w.get(),
           b = new byte[len];

    for (int i = 0; i < len; i++)
      b[i] = mask(s[i]) - 2;
    return b;
  }
}
