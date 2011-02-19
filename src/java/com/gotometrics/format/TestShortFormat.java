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

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class TestShortFormat
{
  private static final int NUM_TESTS = 1024 * 1024;
  private static DataFormat ascendingFormat = ShortFormat.get(),
                            descendingFormat = DescendingShortFormat.get();

  private final Random r;
  private final int numTests;

  public TestShortFormat(Random r, int numTests) {
    this.r = r;
    this.numTests = numTests;
  }

  private short randShort() {
    return (short) r.nextInt();
  }

  private void verifyEncoding(DataFormat format, short x, 
      ImmutableBytesWritable xBytes) 
  {
    short decoded = format.decodeShort(xBytes);
    if (x != decoded)
      throw new RuntimeException("Short 0x" + Integer.toHexString(x) + 
          " decoded as 0x" + Integer.toHexString(decoded));
  }

  private void verifySort(DataFormat format, short x, 
      ImmutableBytesWritable xBytes, short y, ImmutableBytesWritable yBytes)
  {
    int expectedOrder = ((x > y) ? 1 : 0)  - ((x < y) ? 1 : 0);
    int byteOrder = Integer.signum(Bytes.compareTo(xBytes.get(), 
          xBytes.getOffset(), xBytes.getLength(), yBytes.get(), 
          yBytes.getOffset(), yBytes.getLength()));

    if (format.getOrder() == Order.DESCENDING)
      expectedOrder = -expectedOrder;

    if (expectedOrder != byteOrder)
      throw new RuntimeException("Comparing " + x + " to "
          + y + " expected signum " + expectedOrder +
          " got signum " + byteOrder);
  }
                          
  public void test() {
    short x, y;
    ImmutableBytesWritable xBytes = new ImmutableBytesWritable(),
                           yBytes = new ImmutableBytesWritable();

    for (int i  = 0; i < numTests; i++) {
      DataFormat format = r.nextBoolean() ? ascendingFormat : descendingFormat;
      x = randShort();
      y = randShort();
     
      format.encodeShort(x, xBytes);
      format.encodeShort(y, yBytes);

      verifyEncoding(format, x, xBytes);
      verifyEncoding(format, y, yBytes);

      verifySort(format, x, xBytes, y, yBytes);
    }
  }

  private static void usage() {
    System.err.println("Usage: TestShortFormat <-s seed> <-n numTests>");
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

    FileWriter f = new FileWriter("TestShortFormat.out");
    try {
      f.write("-s " + seed + " -n " + numTests);
    } finally {
      f.close();
    }

    TestShortFormat tsf = new TestShortFormat(new Random(seed), numTests);
    tsf.test();

    System.exit(0);
  }
}
