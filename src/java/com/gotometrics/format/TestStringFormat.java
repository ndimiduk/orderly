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
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class TestStringFormat
{
  private static final int NUM_TESTS = 1024 * 1024;
  private static final int MAX_LEN = 65536;

  private static DataFormat ascendingFormat = StringFormat.get(),
                            descendingFormat = DescendingStringFormat.get();

  private final Random r;
  private final int numTests, maxLen;

  public TestStringFormat(Random r, int numTests, int maxLen) {
    this.r = r;
    this.numTests = numTests;
    this.maxLen = maxLen;
  }

  private int randCodePoint() {
    return r.nextInt(Character.MAX_CODE_POINT + 1);
  }

  private String randString() {
    if (r.nextInt(128) == 0)
      return null;

    int len = r.nextInt(maxLen);
    StringBuilder sb = new StringBuilder(len);

    for (int i = 0; i < len; i++) 
      sb.appendCodePoint(randCodePoint());
    return sb.toString();
  }

  private void verifyEncoding(DataFormat format, String s,
      ImmutableBytesWritable sBytes) 
  {
    String decoded = format.decodeString(sBytes);
    if (s == null || decoded == null) {
      if ((s != null) || (decoded != null)) 
        throw new RuntimeException("String " + s + " decoded as " + decoded);
    } else if (!Arrays.equals(Bytes.toBytes(s), Bytes.toBytes(decoded))) {
      throw new RuntimeException("String " + s + " decoded as " + decoded);
    }
  }

  private int stringCompare(String s, String t) {
    if (s == null || t == null)
      return (s != null ? 1 : 0) - (t != null ? 1 : 0);
    return Integer.signum(Bytes.compareTo(Bytes.toBytes(s), 
          Bytes.toBytes(t)));
  }

  private void verifySort(DataFormat format, String s,
      ImmutableBytesWritable sBytes, String t, ImmutableBytesWritable tBytes)
  {
    int expectedOrder = stringCompare(s, t),
        byteOrder = Integer.signum(Bytes.compareTo(sBytes.get(), 
          sBytes.getOffset(), sBytes.getLength(), tBytes.get(), 
          tBytes.getOffset(), tBytes.getLength()));

    if (format.getOrder() == Order.DESCENDING)
      expectedOrder = -expectedOrder;

    if (expectedOrder != byteOrder) 
      throw new RuntimeException("Comparing " + s + " to "
          + t + " expected signum " + expectedOrder +
          " got signum " + byteOrder);
  }
                          
  public void test() {
    String s, t;
    ImmutableBytesWritable sBytes = new ImmutableBytesWritable(),
                           tBytes = new ImmutableBytesWritable();

    for (int i  = 0; i < numTests; i++) {
      DataFormat format = r.nextBoolean() ? ascendingFormat : descendingFormat;
      s = randString();
      t = randString();
     
      format.encodeString(s, sBytes);
      format.encodeString(t, tBytes);

      verifyEncoding(format, s, sBytes);
      verifyEncoding(format, t, tBytes);

      verifySort(format, s, sBytes, t, tBytes);
    }
  }

  private static void usage() {
    System.err.println("Usage: TestStringFormat <-s seed> <-n numTests> " +
        "<-l maxLen>");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException {
    long seed = System.currentTimeMillis();
    int numTests = NUM_TESTS,
        maxLen = MAX_LEN;;

    for (int i = 0; i < args.length; i++) {
      if ("-s".equals(args[i]))
        seed = Long.valueOf(args[++i]);
      else if ("-n".equals(args[i]))
        numTests = Integer.valueOf(args[++i]);
      else if ("-l".equals(args[i]))
        maxLen = Integer.valueOf(args[++i]);
      else usage();
    }

    FileWriter f = new FileWriter("TestStringFormat.out");
    try {
      f.write("-s " + seed + " -n " + numTests + " -l " + maxLen);
    } finally {
      f.close();
    }

    TestStringFormat tsf = new TestStringFormat(new Random(seed), numTests, 
        maxLen);
    tsf.test();

    System.exit(0);
  }
}
