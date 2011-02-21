package com.gotometrics.hbase.rowkey;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public abstract class RowKey 
{
  protected Order order;
  protected boolean isLastKey;
  private ImmutableBytesWritable w;

  public RowKey() {
    this.order = Order.ASC;
    this.isLastKey = true;
  }

  public RowKey setOrder(Order order) {
    this.order = order;
    return this;
  }

  public RowKey setLastKey(boolean isLastKey) { 
    this.isLastKey = isLastKey;
    return this; 
  }

  public Order getOrder() { return order; }

  public boolean isLastKey() { return isLastKey; }

  public abstract Class<?> getSerializedClass();

  public abstract long getSerializedLength(Object o) throws IOException;

  public abstract void serialize(Object o, ImmutableBytesWritable w) 
    throws IOException;

  public void serialize(Object o, byte[] b) throws IOException { 
    this(o, b, 0); 
  }

  public void serialize(Object o, byte[] b, int offset) {
    if (w == null) 
      w = new ImmutableBytesWritable();
    w.set(b, offset, b.length - offset);
    serialize(o, w);
  }

  public byte[] serialize(Object o) throws IOException {
    byte[] b = new byte[getSerializedLength(o)];
    serialize(o, b, 0);
    return b;
  }

  public abstract void skip(ImmutableBytesWritable w) throws IOException;

  public abstract Object deserialize(ImmutableBytesWritable w)
    throws IOException;

  public Object deserialize(byte[] b) throws IOException { 
    return deserialize(b, 0);
  }

  public Object deserialize(byte[] b, int offset) throws IOException {
    if (w == null)
      w = new ImmutableBytesWritable();
    w.set(b, offset, b.length - offset);
    return deserialize(w);
  }

  protected byte mask(byte b) {
    return (byte) (b ^ order.mask());
  }
}
