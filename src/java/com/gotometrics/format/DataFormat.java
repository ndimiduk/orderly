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

package com.gotometrics.format;

import com.gotometrics.thrift.generated.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public abstract class DataFormat 
{
  public String decodeString(ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported decoding",
        getClass(), String.class);
  }

  public byte[] decodeByteArray(ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported decoding",
        getClass(), byte[].class);
  }

  public Date decodeDate(ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported decoding",
        getClass(), Date.class);
  }

  public Time decodeTime(ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported decoding",
        getClass(), Time.class);
  }

  public Timestamp decodeTimestamp(ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported decoding",
        getClass(), Timestamp.class);
  }

  public long decodeLong(ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported decoding",
        getClass(), long.class);
  }

  public int decodeInt(ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported decoding",
        getClass(), int.class);
  }

  public short decodeShort(ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported decoding",
        getClass(), short.class);
  }

  public double decodeDouble(ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported decoding",
        getClass(), double.class);
  }

  public float decodeFloat(ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported decoding",
        getClass(), float.class);
  }

  public BigDecimal decodeBigDecimal(ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported decoding",
        getClass(), BigDecimal.class);
  }

  public void encodeString(String s, ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported encoding",
        getClass(), String.class);
  }

  public void encodeByteArray(byte[] b, ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported encoding",
        getClass(), byte[].class);
  }

  public void encodeDate(Date date, ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported encoding",
        getClass(), Date.class);
  }

  public void encodeTime(Time time, ImmutableBytesWritable bytes) 
  {
    throw new UnsupportedConversionException("Unsupported encoding",
        getClass(), Time.class);
  }

  public void encodeTimestamp(Timestamp ts, ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported encoding",
        getClass(), Timestamp.class);
  }

  public void encodeLong(long l, ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported encoding",
        getClass(), long.class);
  }

  public void encodeInt(int i, ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported encoding",
        getClass(), int.class);
  }

  public void encodeShort(short s, ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported encoding",
        getClass(), short.class);
  }

  public void encodeDouble(double d, ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported encoding",
        getClass(), double.class);
  }

  public void encodeFloat(float f, ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported encoding",
        getClass(), float.class);
  }

  public void encodeBigDecimal(BigDecimal val, ImmutableBytesWritable bytes) {
    throw new UnsupportedConversionException("Unsupported encoding",
        getClass(), BigDecimal.class);
  }

  public abstract Order getOrder(); 

  public abstract int length(ImmutableBytesWritable bytes);
}
