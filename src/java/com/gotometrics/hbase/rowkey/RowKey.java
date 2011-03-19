package com.gotometrics.hbase.rowkey;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Base class for translating objects to/from sort-order preserving byte 
 * arrays. In contrast to other common object serialization methods, 
 * <code>RowKey</code>
 * serializations use a byte array representation that preserves
 * the object's natural sort ordering. Sorting the raw byte arrays yields 
 * the same sort order as sorting the actual objects themselves, without 
 * requiring the object to be instantiated. Using the serialized byte arrays 
 * as HBase row keys ensures that the HBase will sort rows in the natural sort 
 * order of the object. Both primitive and complex/nested 
 * row keys types are supported, and all types support ascending and descending
 * sort orderings.
 */
public abstract class RowKey 
{
  protected Order order;
  private ImmutableBytesWritable w;

  public RowKey() {
    this.order = Order.ASCENDING;
  }

  /** Set the sort order of the row key, ascending or descending. */
  public RowKey setOrder(Order order) {
    this.order = order;
    return this;
  }

  /** Return the sort order of the row key - ascending or descending */
  public Order getOrder() { return order; }

  /** Returns the class of the object used for serialization/deserialization. 
   * @see #serialize
   * @see #deserialize
   */
  public abstract Class<?> getSerializedClass();

  /** Returns the length of the byte array when serializing */
  public abstract int getSerializedLength(Object o) throws IOException;

  /** Serialize object o to ImmutableBytesWritable buffer w. When this
   * method returns, w's position will be adjusted by the number of bytes
   * written. The offset (length) of w is incremented (decremented)
   * by the number of bytes used to serialize o.
   */
  public abstract void serialize(Object o, ImmutableBytesWritable w) 
    throws IOException;

  public void serialize(Object o, byte[] b) throws IOException {
    serialize(o, b, 0); 
  }

  public void serialize(Object o, byte[] b, int offset) throws IOException {
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

  /** Skip over the serialized key in ImmutableBytesWritable w. When this
   * method returns, w's position will be adjusted by the number of bytes
   * in the serialized key. The offset (length) of w is  incremented 
   * (decremented) by the number of bytes in the serialized key.
   */
  public abstract void skip(ImmutableBytesWritable w) throws IOException;

  /** Deserialize the key from ImmutableBytesWritable w. The object is an 
   * instance of the class returned by {@link getSerializedClass}. When this
   * method returns, w's position will be adjusted by the number of bytes
   * in the serialized key. The offset (length) of w is incremented 
   * (decremented) by the number of bytes in the serialized key.
   */
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

  /** Orders serialized byte b by XOR'ing it with the sort order mask. This
   * allows descending sort orders to invert the byte values of the serialized
   * byte stream.
   */
  protected byte mask(byte b) {
    return (byte) (b ^ order.mask());
  }
}
