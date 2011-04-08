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

package com.gotometrics.hbase.rowkey;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serialize and deserialize a struct (record) row key into a sortable
 * byte array. A struct row key is a composed of a fixed number of fields.
 * Each field is a @{link RowKey} of any subtype (including another struct).
 * The struct is sorted by its field values in the order in which the fields 
 * are declared.
 * <p>
 * This is the same general concept as a multi-column primary
 * key or index in MySQL (where the primary key is a struct, and the columns are 
 * field row keys). 
 *
 * <h1> NULL </h1>
 * Structs themselves may not be NULL. However, struct fields may be NULL 
 * (so long as the underlying field row key supports NULL), so you can create a 
 * struct where every field is NULL. 
 *
 * <h1> Descending sort </h1>
 * To sort in descending order we invert the sort order of each field row key.
 *
 * <h1> Usage </h1>
 * Structs impose no extra space during serialization, or object copy overhead
 * at runtime. The storage and runtime costs of a struct row key are the
 * sum of the costs of each of its field row keys. A struct with zero fields 
 * serializes to a zero-byte array.
 */
public class StructRowKey extends RowKey 
{
  private RowKey[] fields;
  private Object[] v;

  /** Creates a struct row key object.
   * @param fields - the field row keys of the struct (in declaration order) 
   */
  public StructRowKey(RowKey[] fields) { this.fields = fields; }

  @Override
  public RowKey setOrder(Order order) {
    if (order == getOrder())
      return this;

    super.setOrder(order);
    for (RowKey field : fields)
      field.setOrder(field.getOrder() == Order.ASCENDING ? Order.DESCENDING : 
          Order.ASCENDING);
    return this;
  }

  @Override
  public Class<?> getSerializedClass() { return Object[].class; }

  protected Object[] toValues(Object obj) {
    Object[] o = (Object[]) obj;
    if (o.length != fields.length)
      throw new IndexOutOfBoundsException("Expected " + fields.length 
         + " values but got " + o.length + " values");
    return o;
  }

  @Override
  public int getSerializedLength(Object obj) throws IOException {
    Object[] o = toValues(obj);
    int len = 0;
    for (int i = 0; i < o.length; i++)
      len += fields[i].getSerializedLength(o[i]);
    return len;
  }

  @Override
  public void serialize(Object obj, ImmutableBytesWritable w) 
    throws IOException
  {
    Object[] o = toValues(obj);
    for (int i = 0; i < o.length; i++)
      fields[i].serialize(o[i], w);
  }

  @Override
  public void skip(ImmutableBytesWritable w) throws IOException {
    for (int i = 0; i < fields.length; i++)
      fields[i].skip(w);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    if (v == null) 
      v = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) 
      v[i] = fields[i].deserialize(w);
    return v;
  }
}
