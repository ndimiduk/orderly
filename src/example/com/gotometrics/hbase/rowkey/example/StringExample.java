package com.gotometrics.hbase.rowkey.example;

import com.gotometrics.hbase.rowkey.StringRowKey;
import com.gotometrics.hbase.rowkey.TextRowKey;
import com.gotometrics.hbase.rowkey.UTF8RowKey;
import com.gotometrics.hbase.rowkey.Order;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class StringExample
{
  /* Simple examples showing serialization lengths with Text key */
  public void lengthExamples() throws Exception {
    TextRowKey i = new TextRowKey();

    System.out.println("serialize(hello) length - " + 
        i.serialize(new Text("hello")).length);
    System.out.println("serialize(null) length - " + i.serialize(null).length);
    System.out.println("serialize('') length - " + 
      i.serialize(new Text("")).length);
    System.out.println("serialize(foobar) length - " + 
      i.serialize(new Text("foobar")).length);

    i.setOrder(Order.DESCENDING);
    System.out.println("descending serialize (null) - length " + 
        i.serialize(null).length);
    System.out.println("descending serialize (hello) - length " + 
        i.serialize(new Text("hello")).length);
    System.out.println("descending serialize ('') - length " + 
        i.serialize(new Text("")).length);
  }

  /* Simple examples showing serialization tests with StringRowKey */
  public void serializationExamples() throws Exception {
    StringRowKey l = new StringRowKey(); 
    ImmutableBytesWritable buffer = new ImmutableBytesWritable();
    byte[] b;

    /* Serialize and deserialize into an immutablebyteswritable */
    b = new byte[l.getSerializedLength("hello")];
    buffer.set(b);
    l.serialize("hello", buffer);
    buffer.set(b);
    System.out.println("deserialize(serialize(hello)) = " + 
        l.deserialize(buffer));

    /* Serialize and deserialize into a byte array (descending sort).  */
    l.setOrder(Order.DESCENDING);
    System.out.println("deserialize(serialize('')) = " + 
        l.deserialize(l.serialize("")));

    /* Serialize and deserialize NULL into a byte array */
    System.out.println("deserialize(serialize(NULL)) = " + 
        l.deserialize(l.serialize(null)));
  }

  public void mustTerminateExamples() throws Exception {
    UTF8RowKey u = new UTF8RowKey();

    System.out.println("length(serialize(foobar)) = " 
        + u.serialize(Bytes.toBytes("foobar")).length);
    System.out.println("deserialize(serialize(foobar)) = " 
        + Bytes.toString((byte[])u.deserialize(u.serialize(
              Bytes.toBytes("foobar")))));

    System.out.println("length(serialize(null)) = " + u.serialize(null).length);
    System.out.println("deserialize(serialize(null)) = " 
        + u.deserialize(u.serialize(null)));

    u.setMustTerminate(true);
    System.out.println("mustTerminate length(serialize(foobar)) = " 
        + u.serialize(Bytes.toBytes("foobar")).length);
    System.out.println("mustTerminate - deserialize(serialize(foobar)) = " 
        + Bytes.toString((byte[])u.deserialize(u.serialize(
              Bytes.toBytes("foobar")))));

    System.out.println("mustTerminate length(serialize(null)) = " + 
        u.serialize(null).length);
    System.out.println("mustTerminate deserialize(serialize(null)) = " 
        + u.deserialize(u.serialize(null)));
  }

  public static void main(String[] args) throws Exception {
    StringExample e = new StringExample();
    e.lengthExamples();
    e.serializationExamples();
    e.mustTerminateExamples();
  }
}
