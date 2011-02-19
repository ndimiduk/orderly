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

package com.gotometrics.hbase.format;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class TestBigDecimalFormat
{
  private static final int MAX_BITS = 1025;
  private static final int NUM_TESTS = 1024 * 1024;
  private static DataFormat ascendingFormat = BigDecimalFormat.get(),
                            descendingFormat = DescendingBigDecimalFormat.get();

  private final Random r;
  private final int numTests;

  public TestBigDecimalFormat(Random r, int numTests) {
    this.r = r;
    this.numTests = numTests;
  }

  private BigInteger randBigInteger() {
    int maxBits = r.nextInt(MAX_BITS);
    switch (r.nextInt(128)) {
      case 0:
        maxBits &= 63;
      case 1:
        maxBits &= 65535;
      case 2:
        maxBits &= ((1 << 21) - 1);
    }

    BigInteger i = new BigInteger(maxBits, r);
    if (r.nextBoolean())
      i = i.negate();
    return i;
  }

  private int randScale(int unscaledBits) {
    int scale = r.nextInt(Integer.MAX_VALUE - unscaledBits);
    if (r.nextBoolean()) scale = -scale;

    switch (r.nextInt(128)) {
      case 0: 
        scale = (scale & 127) - 64;
        break;

      case 1:
        scale = (scale & 16383) - 8192;
        break;

      case 2:
        scale = (scale & ((1 << 21) - 1)) - (1 << 20);
        break;
    }

    return scale;
  }

  private void verifyEncoding(DataFormat format, BigDecimal d,
      ImmutableBytesWritable dBytes) 
  {
    BigDecimal decoded = format.decodeBigDecimal(dBytes);
    d = d.stripTrailingZeros();
    if (d.unscaledValue().equals(BigInteger.ZERO))
      d = BigDecimal.ZERO;
    if (!d.equals(decoded)) 
        throw new RuntimeException("Format " + format.getClass().getName() +
            " BigDecimal " + d + " decoded as " + decoded);
  }

  private void verifySort(DataFormat format, BigDecimal d,
      ImmutableBytesWritable dBytes, BigDecimal e, 
      ImmutableBytesWritable eBytes)
  {
    int expectedOrder = d.compareTo(e);
    int byteOrder = Integer.signum(Bytes.compareTo(dBytes.get(), 
          dBytes.getOffset(), dBytes.getLength(), eBytes.get(), 
          eBytes.getOffset(), eBytes.getLength()));

    if (format.getOrder() == Order.DESCENDING)
      expectedOrder = -expectedOrder;

    if (expectedOrder != byteOrder) {
      System.out.println("d unscaledValue " + d.unscaledValue() + 
          " scale " + d.scale());
      System.out.println("e unscaledValue " + e.unscaledValue() + 
          " scale " + e.scale());
      throw new RuntimeException("Format " + format.getClass().getName() + 
          " Comparing " + d + " to "
          + e + " expected signum " + expectedOrder +
          " got signum " + byteOrder);
    }
  }
                          
  public void test() {
    ImmutableBytesWritable dBytes = new ImmutableBytesWritable(),
                           eBytes = new ImmutableBytesWritable();

    for (int i  = 0; i < numTests; i++) {
      DataFormat format = r.nextBoolean() ? ascendingFormat : descendingFormat;
      BigInteger di = randBigInteger(),
                 ei = randBigInteger();
      int ds = randScale(di.bitCount()),
          es = randScale(ei.bitCount());
      BigDecimal d = new BigDecimal(di, ds),
                 e = new BigDecimal(ei, es);

      format.encodeBigDecimal(d, dBytes);
      format.encodeBigDecimal(e, eBytes);

      verifyEncoding(format, e, eBytes);
      verifyEncoding(format, d, dBytes);

      verifySort(format, d, dBytes, e, eBytes);
    }
  }

  private static void usage() {
    System.err.println("Usage: TestBigDecimalFormat <-s seed> <-n numTests>");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException {
    long seed = System.currentTimeMillis();
    int numTests = NUM_TESTS;

    for (int i = 0; i < args.length; i++) {
      if ("-s".equals(args[i]))
        seed = Long.valueOf(args[++i]);
      else if ("-n".equals(args[i]))
        numTests = Integer.valueOf(args[++i]);
      else usage();
    }

    FileWriter f = new FileWriter("TestBigDecimalFormat.out");
    try {
      f.write("-s " + seed + " -n " + numTests);
    } finally {
      f.close();
    }

    TestBigDecimalFormat tbf = new TestBigDecimalFormat(new Random(seed), 
        numTests);
    tbf.test();

    System.exit(0);
  }
}
