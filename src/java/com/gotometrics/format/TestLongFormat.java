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

public class TestLongFormat
{
  private static final int NUM_TESTS = 1024 * 1024;
  private static LongFormat ascendingFormat = (LongFormat)LongFormat.get(),
                            descendingFormat = 
                              (LongFormat)DescendingLongFormat.get();

  private final Random r;
  private final int numTests;

  public TestLongFormat(Random r, int numTests) {
    this.r = r;
    this.numTests = numTests;
  }

  private Long randLong() {
    if (r.nextInt(128) == 0)
      return null;

    long l = r.nextLong();

    switch (r.nextInt(4)) {
      case 0: /* Single byte: -64 <= x < 64 */
        l = (l & 127) - 64;
        break;

      case 1: /* Double byte: -8192 <= x < 8192 */
        l = (l & 16383) - 8192;
        break;

      case 2: /* 1-2 MB */
        l = (l & ((1 << 21) - 1)) - (1 << 20);
        break;

      /* case 3: do nothing */
    }

    return l;
  }

  private void verifyLongEncoding(LongFormat format, long x, long decoded) {
    if (x != decoded)
      throw new RuntimeException("Long 0x" + Long.toHexString(x) + 
          " decoded as 0x" + Long.toHexString(decoded));
  }

  private void verifyEncoding(LongFormat format, Long x, 
      ImmutableBytesWritable xBytes, boolean isSigned) 
  {
    Long decoded = isSigned ? format.decodeNullableLong(xBytes) :
      format.decodeNullableUnsignedLong(xBytes);
    if (x != null && decoded != null) {
      verifyLongEncoding(format, x, decoded);
      return;
    }

    if (x != null || decoded != null)
      throw new RuntimeException("Long " + x + " decoded as " + decoded);
  }

  private int longCompare(Long x, Long y, boolean isSigned) {
    if (x == null || y == null)
      return (x != null ? 1 : 0) - (y != null ? 1 : 0);

    /* convert unsigned comparison to signed */
    if (!isSigned) {
      x = new Long(x ^ Long.MIN_VALUE);
      y = new Long(y ^ Long.MIN_VALUE);
    }

    return ((x > y) ? 1 : 0)  - ((x < y) ? 1 : 0);
  }

  private void verifySort(LongFormat format, Long x, 
      ImmutableBytesWritable xBytes, Long y, ImmutableBytesWritable yBytes,
      boolean isSigned)
  {
    int expectedOrder = longCompare(x, y, isSigned),
        byteOrder = Integer.signum(Bytes.compareTo(xBytes.get(), 
          xBytes.getOffset(), xBytes.getLength(), yBytes.get(), 
          yBytes.getOffset(), yBytes.getLength()));

    if (format.getOrder() == Order.DESCENDING)
      expectedOrder = -expectedOrder;

    if (expectedOrder != byteOrder)
      throw new RuntimeException("Comparing " + x + " to "
          + y + " expected signum " + expectedOrder +
          " got signum " + byteOrder + " isSigned " + isSigned);
  }
                          
  public void test() {
    Long x, y;
    ImmutableBytesWritable xBytes = new ImmutableBytesWritable(),
                           yBytes = new ImmutableBytesWritable();

    for (int i  = 0; i < numTests; i++) {
      LongFormat format = r.nextBoolean() ? ascendingFormat : descendingFormat;
      x = randLong();
      y = randLong();

      boolean isSigned = r.nextBoolean(); 
      if (isSigned) {
        format.encodeNullableLong(x, xBytes);
        format.encodeNullableLong(y, yBytes);
      } else {
        format.encodeNullableUnsignedLong(x, xBytes);
        format.encodeNullableUnsignedLong(y, yBytes);
      }

      verifyEncoding(format, x, xBytes, isSigned);
      verifyEncoding(format, y, yBytes, isSigned);

      verifySort(format, x, xBytes, y, yBytes, isSigned);
    }
  }

  private static void usage() {
    System.err.println("Usage: TestLongFormat <-s seed> <-n numTests>");
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

    FileWriter f = new FileWriter("TestLongFormat.out");
    try {
      f.write("-s " + seed + " -n " + numTests);
    } finally {
      f.close();
    }

    TestLongFormat tlf = new TestLongFormat(new Random(seed), numTests);
    tlf.test();

    System.exit(0);
  }
}
