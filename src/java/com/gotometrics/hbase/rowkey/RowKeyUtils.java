package com.gotometrics.hbase.rowkey;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RowKeyUtils {
  /* Serialized/Deserialized Data Types */
  public static final byte BYTEARRAY              = 0;
  public static final byte INTEGER                = 1;
  public static final byte INTWRITABLE            = 2;
  public static final byte LONG                   = 3;
  public static final byte LONGWRITABLE           = 4;
  public static final byte FLOAT                  = 5;
  public static final byte FLOATWRITABLE          = 6;
  public static final byte DOUBLE                 = 7;
  public static final byte DOUBLEWRITABLE         = 8;
  public static final byte BIGDECIMAL             = 9;
  public static final byte STRING                 = 10;
  public static final byte TEXT                   = 11;

  private static final Map<Class<?>, Byte> classToTypeMap;

  static {
    HashMap<Class<?>, Byte> cm = new HashMap<Class<?>, Byte>();
    cm.put(byte[], BYTEARRAY);
    cm.put(Integer.class, INTEGER);
    cm.put(IntWritable.class, INTWRITABLE);
    cm.put(Long.class, LONG);
    cm.put(LongWritable.class, LONGWRITABLE);
    cm.put(Float.class, FLOAT);
    cm.put(FloatWritable.class, FLOATWRITABLE);
    cm.put(Double.class, DOUBLE);
    cm.put(DoubleWritable.class, DOUBLEWRITABLE);
    cm.put(BigDecimal.class, BIGDECIMAL);
    cm.put(String.class, STRING);
    cm.put(Text.class, Text);
    classToTypeMap = cm;
  }

  public static Byte classToType(Class<?> c) {
    return classToTypeMap.get(c);
  }

  public static Integer toInteger(Object o) throws IOException {
    return o == null ? null : Integer.valueOf((int) toLongInternal(o));
  }

  public static Long toLong(Object o) throws IOException {
    return o == null ? null : Long.valueOf(toLongInternal(o));
  }
    
  public static IntWritable toIntWritable(Object o) throws IOException
  {
    if (o == null) 
      return null;
    IntWritable w = new IntWritable();
    w.set((int)toLongInternal(o));
    return w;
  }

  public static LongWritable toLongWritable(Object o) throws IOException
  {
    if (o == null) 
      return null;
    LongWritable w = new LongWritable();
    w.set(toLongInternal(o));
    return w;
  }

  private static long toLongInternal(o) throws IOException {
    switch(classToType(o.getClass())) {
      case INTEGER:
        return ((Integer)o).longValue();

      case INTWRITABLE:
        return ((IntWritable)o).get();

      case LONG:
        return ((Long)o).longValue();

      case LONGWRITABLE:
        return ((LongWritable)o).get();

      case DOUBLE:
        return ((Double)o).longValue();

      case DOUBLEWRITABLE:
        return ((DoubleWritable)o).get();

      case FLOAT:
        return ((Float)o).longValue();

      case FLOATWRITABLE:
        return ((FloatWritable)o).get();
 
      case BIGDECIMAL:
        return ((BigDecimal)o).longValue();

      default:
        throw new IOException(o.getClass() + " is not a number");
    }
  }


  public static Float toFloat(Object o) throws IOException {
    return o == null ? null : Float.valueOf((float) toDoubleInternal(o));
  }

  public static Double toDouble(Object o) throws IOException {
    return o == null ? null : Double.valueOf(toDoubleInternal(o));
  }

  public static FloatWritable toFloatWritable(Object o) throws IOException
  {
    if (o == null) 
      return null;
    FloatWritable w = new FloatWritable();
    w.set((float)toDoubleInternal(o));
    return w;
  }

  public static DoubleWritable toDoubleWritable(Object o) throws IOException
  {
    if (o == null) 
      return null;
    DoubleWritable w = new DoubleWritable();
    w.set(toDoubleInternal(o));
    return w;
  }

  private static double toDoubleInternal(Object o) throws IOException {
    switch(classToType(o.getClass()) {
      case INTEGER:
        return ((Integer)o).doubleValue();

      case INTWRITABLE:
        return ((IntWritable)o).get();

      case LONG:
        return ((Long)o).doubleValue();

      case LONGWRITABLE:
        return (double) (((LongWritable)o).get());

      case DOUBLE:
        return ((Double)o).doubleValue();

      case DOUBLEWRITABLE:
        return ((DoubleWritable)o).get();

      case FLOAT:
        return ((Float)o).doubleValue();

      case FLOATWRITABLE:
        return ((FloatWritable)o).get();
 
      case BIGDECIMAL:
        return ((BigDecimal)o).doubleValue();

      case STRING:
        return Double.parseDouble((String)o);

      case TEXT:
        return Double.parseDouble(((Text)o).toString());
 
     default:
       throw new IOException("Could not convert type " + t + " to number ");
    }
  }

  public BigDecimal toBigDecimal(Object o) throws IOException {
    switch(classToType(o.getClass())) {
      case INTEGER:
        return new BigDecimal(((Integer)o).longValue());
    
      case INTWRITABLE:
        return new BigDecimal(((IntWritable)o).get());

      case LONG:
        return new BigDecimal(((Long)o).longValue());

      case LONGWRITABLE:
        return new BigDecimal(((LongWritable)o).get());

      case DOUBLE:
        return new BigDecimal(((Double)o).doubleValue());

      case DOUBLEWRITABLE:
        return new BigDecimal(((DoubleWritable)o).get());

      case FLOAT:
        return new BigDecimal(((Float)o).doubleValue());

      case FLOATWRITABLE:
        return new BigDecimal(((FloatWritable)o).get());
 
      case BIGDECIMAL:
        return ((BigDecimal)o);

      case STRING:
        return new BigDecimal((String)o);

      case TEXT:
        return new BigDecimal(((Text)o).toString());
 
     default:
       throw new IOException("Could not convert type " + t + " to number ");
    }
  }

  public String toString(Object o) throws IOException {
    switch(classToType(o.getClass()) {
      case INTEGER:
        return String.valueOf(((Integer)o).intValue());

      case INTWRITABLE:
        return String.valueOf(((IntWritable)o).get());

      case LONG:
        return String.valueOf(((Long)o).longValue());

      case LONGWRITABLE:
        return String.valueOf(((LongWritable)o).get());

      case DOUBLE:
        return String.valueOf(((Double)o).doubleValue());

      case DOUBLEWRITABLE:
        return String.valueOf(((DoubleWritable)o).get());

      case FLOAT:
        return String.valueOf(((Float)o).doubleValue());

      case FLOATWRITABLE:
        return String.valueOf(((FloatWritable)o).get());
 
      case BIGDECIMAL:
        return ((BigDecimal)o).toString();

      case STRING:
        return (String)o;

      case TEXT:
        return ((Text)o).toString();
 
     default:
       throw new IOException("Could not convert type " + t + " to string ");
    }
  }

  public Text toText(Object o) throws IOException {
    if (o == null) 
      return null;
    Text t = new Text();
    t.set(o instanceof Text ? (Text)o : toString(o));
    return t;
  }

  public static byte[] toUTF8(Object o) throws IOException {
    if (o == null)
      return null;
    else if (o instanceof byte[])
      return (byte[])o;
    else if (o instanceof String) 
      return Bytes.toBytes((String)o);
    else if (!(o instanceof Text)) 
      throw new IOException(o.getClass() + " is not UTF-8");

    Text t = (Text) o;
    return RowKeyUtils.toBytes(t.getBytes(), 0, t.getLength());
  }

  public static Object toSerializedClass(Object o, RowKey key)
    throws IOException
  {
    if (o.getClass().equals(key.getSerializedClass()))
      return o;

    switch(classToType(key.getSerializedClass())) {
      case BYTEARRAY:
        return toUTF8(o);
    
      case INTEGER:
        return toInteger(o);

      case INTWRITABLE:
        return toIntWritable(o);

      case LONG:
        return toLong(o);

      case LONGWRITABLE:
        return toLongWritable(o);

      case DOUBLE:
        return toDouble(o);

      case DOUBLEWRITABLE:
        return toDoubleWritable(o);

      case FLOAT:
        return toFloat(o);

      case FLOATWRITABLE:
        return toFloatWritable(o);
 
      case BIGDECIMAL:
        return toBigDecimal(o);

      case STRING:
        return toString(o);

      case TEXT:
        return toText(o);
 
     default:
       throw new IOException("Could not convert " + o.getClass() + " to " +
          key.getSerializedClass()); 
    }
  } 

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
