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
import java.sql.Date;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class TestDateFormat
{
  private static final int NUM_TESTS = 1024 * 1024;
  private static DataFormat ascendingFormat = DateFormat.get(),
                            descendingFormat = DescendingDateFormat.get();

  private final Random r;
  private final int numTests;

  public TestDateFormat(Random r, int numTests) {
    this.r = r;
    this.numTests = numTests;
  }

  private Date randDate() {
    return new Date(r.nextLong());
  }

  private void verifyEncoding(DataFormat format, Date d, 
      ImmutableBytesWritable dBytes) 
  {
    Date decoded = format.decodeDate(dBytes);
    if (!d.equals(decoded))
      throw new RuntimeException("Date " + d + " decoded as " + decoded);
  }

  private void verifySort(DataFormat format, Date d,
      ImmutableBytesWritable dBytes, Date e, ImmutableBytesWritable eBytes)
  {
    int expectedOrder = Integer.signum(d.compareTo(e)),
        byteOrder = Integer.signum(Bytes.compareTo(dBytes.get(), 
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
    Date d, e;
    ImmutableBytesWritable dBytes = new ImmutableBytesWritable(),
                           eBytes = new ImmutableBytesWritable();

    for (int i  = 0; i < numTests; i++) {
      DataFormat format = r.nextBoolean() ? ascendingFormat : descendingFormat;
      d = randDate();
      e = randDate();
    
      format.encodeDate(d, dBytes);
      format.encodeDate(e, eBytes);

      verifyEncoding(format, d, dBytes);
      verifyEncoding(format, e, eBytes);

      verifySort(format, d, dBytes, e, eBytes);
    }
  }

  private static void usage() {
    System.err.println("Usage: TestDateFormat <-s seed> <-n numTests>");
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

    FileWriter f = new FileWriter("TestDateFormat.out");
    try {
      f.write("-s " + seed + " -n " + numTests);
    } finally {
      f.close();
    }

    TestDateFormat tdf = new TestDateFormat(new Random(seed), numTests);
    tdf.test();

    System.exit(0);
  }
}
