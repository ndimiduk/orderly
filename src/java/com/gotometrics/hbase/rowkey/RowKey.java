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
  protected boolean mustTerminate;
  private ImmutableBytesWritable w;

  public RowKey() { this.order = Order.ASCENDING; }

  /** Sets the sort order of the row key - ascending or descending. 
   */ 
  public RowKey setOrder(Order order) { this.order = order; return this; }

  /** Gets the sort order of the row key - ascending or descending */
  public Order getOrder() { return order; }

  /** Returns true if the row key serialization must be explicitly terminated 
   * in some fashion (such as a terminator byte or a self-describing length).
   * If this is false, the end of the byte array may serve as an implicit 
   * terminator. Defaults to false.
   */
  public boolean mustTerminate() { return mustTerminate; }

  /** Sets the mustTerminate flag for this row key. If this flag is false,
   * the end of the byte array can be used to terminate encoded values. You
   * should only set this value if you are adding a custom byte value suffix
   * to a row key.
   */
  public RowKey setMustTerminate(boolean mustTerminate) {
    this.mustTerminate = mustTerminate;
    return this;
  }

  /** Returns true if termination is required */
  boolean terminate() { return mustTerminate || order == Order.DESCENDING; }

  /** Gets the class of the object used for serialization.
   * @see #serialize
   */
  public abstract Class<?> getSerializedClass();

  /** Gets the class of the object used for deserialization.
   * @see #deserialize
   */
  public Class<?> getDeserializedClass() { return getSerializedClass(); }

  /** Gets the length of the byte array when serializing an object.
   * @param o object to serialize
   * @return the length of the byte array used to serialize o
   */
  public abstract int getSerializedLength(Object o) throws IOException;

  /** Serializes an object o to a byte array. When this
   * method returns, the byte array's position will be adjusted by the number 
   * of bytes written. The offset (length) of the byte array is incremented 
   * (decremented) by the number of bytes used to serialize o.
   * @param o object to serialize
   * @param w byte array used to store the serialized object
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

  /** Skips over a serialized key in the byte array. When this
   * method returns, the byte array's position will be adjusted by the number of
   * bytes in the serialized key. The offset (length) of the byte array is 
   * incremented (decremented) by the number of bytes in the serialized key.
   * @param w the byte array containing the serialized key
   */
  public abstract void skip(ImmutableBytesWritable w) throws IOException;

  /** Deserializes a key from the byte array. The returned object is an 
   * instance of the class returned by {@link getSerializedClass}. When this
   * method returns, the byte array's position will be adjusted by the number of
   * bytes in the serialized key. The offset (length) of the byte array is 
   * incremented (decremented) by the number of bytes in the serialized key.
   * @param w the byte array used for key deserialization
   * @return the deserialized key from the current position in the byte array
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
