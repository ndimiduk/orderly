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
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class TestDoubleFormat
{
  private static final int NUM_TESTS = 1024 * 1024;
  private static DataFormat ascendingFormat = DoubleFormat.get(),
                            descendingFormat = DescendingDoubleFormat.get();

  private final Random r;
  private final int numTests;

  public TestDoubleFormat(Random r, int numTests) {
    this.r = r;
    this.numTests = numTests;
  }

  private double randDouble() {
    switch (r.nextInt(128)) {
      case 0:
        return +0.0d;
      case 1:
        return -0.0d;
      case 2:
        return Double.POSITIVE_INFINITY;
      case 3:
        return Double.NEGATIVE_INFINITY;
      case 4:
        return Double.NaN;
    }

    return r.nextDouble();
  }

  /* Returns 1 if +0.0, 0 otherwise */
  private int isPositiveZero(double d) {
    return 1/d == Double.POSITIVE_INFINITY ? 1 : 0;
  }

  private void verifyEncoding(DataFormat format, double d,
      ImmutableBytesWritable dBytes) 
  {
    double decoded = format.decodeDouble(dBytes);
    if (!Double.isNaN(d) && d != 0.0d) {
      if (d != decoded)
        throw new RuntimeException("Double " + d + " decoded as " + decoded);
      return;
    } else if (Double.isNaN(d)) {
      if (!Double.isNaN(decoded))
        throw new RuntimeException("Double " + d + " decoded as " + decoded);
      return;
    } else if (d != decoded || isPositiveZero(d) != isPositiveZero(decoded)) 
        throw new RuntimeException("Double " + d + " decoded as " + decoded);
  }

  int doubleCompare(double d, double e) {
    if (!Double.isNaN(d) && !Double.isNaN(e) && !(d == 0 && e == 0)) 
      return ((d > e) ? 1 : 0) - ((e > d) ? 1 : 0);

    if (Double.isNaN(d)) {
      if (Double.isNaN(e))
        return 0;
      return 1;
    } else if (Double.isNaN(e)) {
      return -1;
    } else /* d == +/-0.0 && e == +/-0.0 */ {
      return isPositiveZero(d) - isPositiveZero(e);
    }
  }

  private void verifySort(DataFormat format, double d,
      ImmutableBytesWritable dBytes, double e, ImmutableBytesWritable eBytes)
  {
    int expectedOrder = doubleCompare(d, e);
    int byteOrder = Integer.signum(Bytes.compareTo(dBytes.get(), 
          dBytes.getOffset(), dBytes.getLength(), eBytes.get(), 
          eBytes.getOffset(), eBytes.getLength()));

    if (format.getOrder() == Order.DESCENDING)
      expectedOrder = -expectedOrder;

    if (expectedOrder != byteOrder)
      throw new RuntimeException("Comparing " + d + " to "
          + e + " expected signum " + expectedOrder +
          " got signum " + byteOrder);
  }
                          
  public void test() {
    double d, e;
    ImmutableBytesWritable dBytes = new ImmutableBytesWritable(),
                           eBytes = new ImmutableBytesWritable();

    for (int i  = 0; i < numTests; i++) {
      DataFormat format = r.nextBoolean() ? ascendingFormat : descendingFormat;
      d = randDouble();
      e = randDouble();

      /* Canonicalize NaNs */
      d = Double.longBitsToDouble(Double.doubleToLongBits(d));
      e = Double.longBitsToDouble(Double.doubleToLongBits(e));
    
      format.encodeDouble(d, dBytes);
      format.encodeDouble(e, eBytes);

      verifyEncoding(format, d, dBytes);
      verifyEncoding(format, e, eBytes);

      verifySort(format, d, dBytes, e, eBytes);
    }
  }

  private static void usage() {
    System.err.println("Usage: TestDoubleFormat <-s seed> <-n numTests>");
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

    FileWriter f = new FileWriter("TestDoubleFormat.out");
    try {
      f.write("-s " + seed + " -n " + numTests);
    } finally {
      f.close();
    }

    TestDoubleFormat tdf = new TestDoubleFormat(new Random(seed), numTests);
    tdf.test();

    System.exit(0);
  }
}
