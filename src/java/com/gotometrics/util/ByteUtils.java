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

import com.gotometrics.hbase.format.DataFormat;
import com.gotometrics.hbase.format.DoubleFormat;
import com.gotometrics.hbase.format.LongFormat;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class ByteUtils
{
  public static final byte[] EMPTY = new byte[0];

  public static byte[] toBytes(DataOutputBuffer out) {
    byte[] b = out.getData();
    if (b.length == out.getLength())
      return b;
    return Arrays.copyOf(b, out.getLength());
  }

  public static byte[] toBytes(ImmutableBytesWritable ibw) {
    byte[] b = ibw.get();
    int offset = ibw.getOffset(),
        len = ibw.getLength();

    if (offset == 0 && len == b.length)
      return b;
    return Arrays.copyOfRange(b, offset, offset + len);
  }

  public static byte[] toBytes(Writable writable) {
    final DataOutputBuffer out = new DataOutputBuffer();
    try {
      writable.write(out);
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert writable to byte array",e);
    } 

    return toBytes(out);
  }

  public static <T extends Writable> T toWritable(Class<T> c, byte[] bytes)
  {
    final DataInputBuffer in = new DataInputBuffer();
    T writable = ReflectionUtils.newInstance(c, null); 
    in.reset(bytes, bytes.length);

    try {
      writable.readFields(in);
      in.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert byte array to writable",e);
    } 

    return writable;
  }

  public static byte[] toBytesGeneric(Writable w) throws IOException 
  {
    final DataOutputBuffer out = new DataOutputBuffer();
    try {
      out.writeUTF(w.getClass().getName());
      w.write(out);
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert writable to byte array",e);
    } 

    return toBytes(out);
  }

  @SuppressWarnings("unchecked")
  public static <T extends Writable> T toWritableGeneric(byte[] bytes) 
   throws IOException, ClassNotFoundException
 {
    final DataInputBuffer in = new DataInputBuffer();
    in.reset(bytes, bytes.length);

    try {
      Class<?> c = Class.forName(in.readUTF());
      T w = (T) ReflectionUtils.newInstance(c, null); 
      w.readFields(in);
      in.close();
      return w;
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert byte array to writable",e);
    } 
  }

  public static void copyBytes(ImmutableBytesWritable src,
                               ImmutableBytesWritable dst)
  {
    int len = src.getLength();
    System.arraycopy(src.get(), src.getOffset(), dst.get(), dst.getOffset(),
                     len);
    dst.set(dst.get(), dst.getOffset() + len, dst.getLength() - len);
  }

  public static void skipValue(DataFormat format, ImmutableBytesWritable ibw) {
    int len = format.length(ibw);
    ibw.set(ibw.get(), ibw.getOffset() + len, ibw.getLength() - len);
  }

  public static ImmutableBytesWritable toBytes(long l) {
    ImmutableBytesWritable bytes = new ImmutableBytesWritable();
    LongFormat.get().encodeLong(l, bytes);
    return bytes;
  }

  public static long toLong(ImmutableBytesWritable bytes) {
    return LongFormat.get().decodeLong(bytes);
  }

  public static ImmutableBytesWritable toBytes(double d) {
    ImmutableBytesWritable bytes = new ImmutableBytesWritable();
    DoubleFormat.get().encodeDouble(d, bytes);
    return bytes;
  }

  public static double toDouble(ImmutableBytesWritable bytes) {
    return DoubleFormat.get().decodeDouble(bytes);
  }
}
