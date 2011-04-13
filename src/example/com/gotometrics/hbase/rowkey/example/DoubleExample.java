package com.gotometrics.hbase.rowkey.example;

import com.gotometrics.hbase.rowkey.DoubleRowKey;
import com.gotometrics.hbase.rowkey.DoubleWritableRowKey;
import com.gotometrics.hbase.rowkey.Order;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class DoubleExample
{
  /* Simple examples showing serialization lengths with Double Row Key */
  public void lengthExamples() throws Exception {
    DoubleRowKey d = new DoubleRowKey();

    System.out.println("serialize(null) length - " + d.serialize(null).length);
    System.out.println("serialize(57.190235) length - " + 
        d.serialize(57.923924).length);
    System.out.println("serialize(1000000.999) length - " + 
        d.serialize(1000000.99).length);

    d.setOrder(Order.DESCENDING);
    System.out.println("descending serialize (null) - length " + 
        d.serialize(null).length);
    System.out.println("descending serialize (57) - length " + 
        d.serialize(57d).length);
  }

  /* Simple examples showing serialization tests with DoubleWritable Row Key */
  public void serializationExamples() throws Exception {
    DoubleWritableRowKey d = new DoubleWritableRowKey();
    DoubleWritable w = new DoubleWritable();
    ImmutableBytesWritable buffer = new ImmutableBytesWritable();
    byte[] b;

    /* Serialize and deserialize into an immutablebyteswritable */
    w.set(-93214.920352);
    b = new byte[d.getSerializedLength(w)];
    buffer.set(b);
    d.serialize(w, buffer);
    buffer.set(b, 0, b.length);
    System.out.println("deserialize(serialize(-93214.920352)) = " + 
        ((DoubleWritable)d.deserialize(buffer)).get());

    /* Serialize and deserialize into a byte array (descending sort)
     */
    d.setOrder(Order.DESCENDING);
    w.set(0);
    System.out.println("deserialize(serialize(0)) = " + 
        ((DoubleWritable)d.deserialize(d.serialize(w))).get());

    /* Serialize and deserialize NULL into a byte array */
    System.out.println("deserialize(serialize(NULL)) = " + 
        d.deserialize(d.serialize(null)));
  }

  public static void main(String[] args) throws Exception {
    DoubleExample e = new DoubleExample();
    e.lengthExamples();
    e.serializationExamples();
  }
}
