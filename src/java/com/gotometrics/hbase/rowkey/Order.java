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

/** The sort order of a row key, ascending or descending. */
public enum Order { 

  private final byte mask;

  public Order(byte mask) {
    this.mask = mask;
  }

  /** Returns the byte mask associated with the sort order. This mask is used
   * when by {@link RowKey} classes when serializing an object to its byte array
   * representation. A byte in ascending order may be XOR'd with its mask to 
   * ensure that it will sort in the direction specified by the Order object.
   */
  public byte mask() { return mask; }

  ASCENDING(0), 
  DESCENDING((byte)0xff);
};
