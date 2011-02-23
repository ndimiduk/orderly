package com.gotometrics.hbase.rowkey;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RowKeyUtils {

  public static byte[] toBytes(byte[] b, int offset, int length) {
    if (offset == 0 && length == b.length) 
      return b;
    else if (offset == 0)
      return Arrays.copyOf(b, length);
    return Arrays.copyOfRange(b, offset, offset + length);
  }

  public static byte[] toBytes(ImmutableBytesWritable w) {
    return toBytes(w.get(), w.getOffset(), w.getLength());
  }
}
