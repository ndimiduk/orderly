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
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class RandomRowKeyTestCase extends RowKeyTestCase
{
  protected Random r;
  protected int numTests, maxRedZone;

  @Before
  @Override
  public void setUp() {
    r = new Random(Long.valueOf(System.getProperty("test.random.seed", "0")));
    numTests = Integer.valueOf(System.getProperty("test.random.count", "8192"));
    maxRedZone = Integer.valueOf(System.getProperty("test.random.maxredzone", 
          "16"));
    super.setUp();
  }

  @After
  @Override
  public void tearDown() { 
    this.r = null;
    super.tearDown();
  }

  public RandomRowKeyTestCase setRandom(Random r) {
    this.r = r;
    return this;
  }

  public RandomRowKeyTestCase setNumTests(int numTests) {
    this.numTests = numTests;
    return this;
  }

  public RandomRowKeyTestCase setMaxRedZone(int maxRedZone) {
    this.maxRedZone = maxRedZone;
    return this;
  }

  @Override
  public ImmutableBytesWritable allocateBuffer(Object o) throws IOException {
    return new RedZoneImmutableBytesWritable(key.getSerializedLength(o));
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException
  {
    byte[] b;
    int len;

    switch(r.nextInt(4)) {
      case 0: /* serialize(Object, ImmutableBytesWritable) */
        key.serialize(o, w);
        break;

      case 1: /* serialize(Object) */
        b = key.serialize(o);
        System.arraycopy(b, 0, w.get(), w.getOffset(), b.length);
        w.set(w.get(), w.getOffset() + b.length, w.getLength() - b.length);
        break;

      case 2: /* serialize(Object, byte[]) */
        len = key.getSerializedLength(o);
        b = new byte[len];
        key.serialize(o, b);
        System.arraycopy(b, 0, w.get(), w.getOffset(), len);
        w.set(w.get(), w.getOffset() + len, w.getLength() - len);
        break;

      default: /* serialize(Object, byte[], offset) */
        key.serialize(o, w.get(), w.getOffset());
        len = key.getSerializedLength(o);
        w.set(w.get(), w.getOffset() + len, w.getLength() - len);
        break;
    }
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException 
  {
    Object o;

    switch(r.nextInt(3)) {
      case 0: /* deserialize(ImmutableBytesWritable) */
        o = deserialize(w);
        break;

      case 1: /* deserialize(byte[] b) */
        o = key.deserialize(Arrays.copyOfRange(w.get(), w.getOffset(), 
              w.getOffset() + w.getLength()));
        key.skip(w);
        break;

      default: /* deserialize(byte[] b, int offset) */
        o = key.deserialize(Arrays.copyOfRange(w.get(), w.getOffset(),
              w.getOffset() + w.getLength()));
        key.skip(w);
        break;
    }

    return o;
  }

  @Override
  public void testSerialization(Object o, ImmutableBytesWritable w) 
    throws IOException 
  {
    super.testSerialization(o, w);
    ((RedZoneImmutableBytesWritable)w).verify();
  }

  @Override
  public void testSkip(Object o, ImmutableBytesWritable w) 
    throws IOException
  {
    super.testSkip(o, w);
    ((RedZoneImmutableBytesWritable)w).verify();
  }

  @Override
  public void testSort(Object o1, ImmutableBytesWritable w1, Object o2, 
      ImmutableBytesWritable w2) throws IOException
  {
    super.testSort(o1, w1, o2, w2);
    ((RedZoneImmutableBytesWritable)w1).verify();
    ((RedZoneImmutableBytesWritable)w2).verify();
  }
    
  @Test
  @Override
  public void testRowKey() throws IOException {
    for (int i = 0; i < numTests; i++) {
      setRowKey(createRowKey());
      super.testRowKey();
    }
  }

  protected class RedZoneImmutableBytesWritable 
    extends ImmutableBytesWritable
  {
    byte[] header, trailer;
    int buflen;

    public RedZoneImmutableBytesWritable() { }

    public RedZoneImmutableBytesWritable(int len) { reset(len); }

    private void randomize(byte[] b, int offset, int len) {
      for (int i = offset; i < len; i++)
        b[i] = (byte) r.nextInt();
    }

    private void randomize(byte[] b) {
      randomize(b, 0, b.length);
    }

    public RedZoneImmutableBytesWritable reset(int len) {
      if (maxRedZone > 0) {
        header = new byte[r.nextInt(maxRedZone)];
        trailer = new byte[r.nextInt(maxRedZone)];
        randomize(header);
        randomize(trailer);
      } else {
        header = RowKeyUtils.EMPTY;
        trailer = RowKeyUtils.EMPTY;
      }

      byte[] b = new byte[header.length + len + trailer.length];
      System.arraycopy(header, 0, b, 0, header.length);
      System.arraycopy(trailer, 0, b, header.length + len, trailer.length);
      randomize(b, header.length, len);

      set(b, header.length, len);
      this.buflen = len;
      return this;
    }

    private void verifyEquals(byte[] a, int aOffset, byte[] b, 
        int bOffset, int len) 
    {
      for (int i = 0; i < len; i++)
        assertEquals("Header/Trailer corruption", a[aOffset + i], 
            b[bOffset + i]);
    }

    public void verify() {
      verifyEquals(header, 0, get(), 0, header.length);
      verifyEquals(trailer, 0, get(), header.length + buflen, trailer.length);
    }
  }
}
