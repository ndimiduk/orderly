package com.gotometrics.hbase.rowkey.example;

import com.gotometrics.hbase.rowkey.FixedUnsignedLongRowKey;
import com.gotometrics.hbase.rowkey.FixedLongWritableRowKey;
import com.gotometrics.hbase.rowkey.Order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class FixedLongExample
{
  /* Simple examples showing serialization lengths with 
   * FixedUnsignedLongRow Key 
   */
  public void lengthExamples() throws Exception {
    FixedUnsignedLongRowKey i = new FixedUnsignedLongRowKey();

    System.out.println("serialize(2^63) length - " + 
        i.serialize(Long.MIN_VALUE).length);
    System.out.println("serialize(57) length - " + i.serialize(57l).length);
    System.out.println("serialize(293) length - " + i.serialize(293l).length);

    i.setOrder(Order.DESCENDING);
    System.out.println("descending serialize (57) - length " + 
        i.serialize(57l).length);
    System.out.println("descending serialize (2^32) - length " + 
        i.serialize(1l << 32).length);
  }

  /* Simple examples showing serialization tests with FixedLongWritable */
  public void serializationExamples() throws Exception {
    FixedLongWritableRowKey l = new FixedLongWritableRowKey(); 
    LongWritable w = new LongWritable();
    ImmutableBytesWritable buffer = new ImmutableBytesWritable();
    byte[] b;

    /* Serialize and deserialize into an immutablebyteswritable */
    w.set(-93214);
    b = new byte[l.getSerializedLength(w)];
    buffer.set(b);
    l.serialize(w, buffer);
    buffer.set(b, 0, b.length);
    System.out.println("deserialize(serialize(-93214)) = " + 
        ((LongWritable)l.deserialize(buffer)).get());

    /* Serialize and deserialize into a byte array (descending sort).  */
    l.setOrder(Order.DESCENDING);
    w.set(0);
    System.out.println("deserialize(serialize(0)) = " + 
        ((LongWritable)l.deserialize(l.serialize(w))).get());
  }

  public static void main(String[] args) throws Exception {
    FixedLongExample e = new FixedLongExample();
    e.lengthExamples();
    e.serializationExamples();
  }
}
