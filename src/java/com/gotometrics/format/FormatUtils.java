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

package com.gotometrics.hbase.format;


import java.util.HashMap;
import java.util.Map;

public class FormatUtils
{
  private static final Map<String,DataFormat> toFormatFromName;
  private static final Map<DataFormat, String> toName;

  static {
    Map<String,DataFormat> fmtName = 
      new HashMap<String, DataFormat>();
    fmtName.put("bigdecimal", BigDecimalFormat.get());
    fmtName.put("double", DoubleFormat.get());
    fmtName.put("float", FloatFormat.get());
    fmtName.put("int", IntFormat.get());
    fmtName.put("long", LongFormat.get());
    fmtName.put("short", ShortFormat.get());
    fmtName.put("string", StringFormat.get());

    fmtName.put("descendingbigdecimal", DescendingBigDecimalFormat.get());
    fmtName.put("descendingdouble", DescendingDoubleFormat.get());
    fmtName.put("descendingfloat", DescendingFloatFormat.get());
    fmtName.put("descendingint", DescendingIntFormat.get());
    fmtName.put("descendinglong", DescendingLongFormat.get());
    fmtName.put("descendingshort", DescendingShortFormat.get());
    fmtName.put("descendingstring", DescendingStringFormat.get());
    toFormatFromName = fmtName;

    Map<DataFormat, String> name = 
      new HashMap<DataFormat, String>();

    name.put(BigDecimalFormat.get(), "bigdecimal");
    name.put(DoubleFormat.get(), "double");
    name.put(FloatFormat.get(), "float");
    name.put(IntFormat.get(), "int");
    name.put(LongFormat.get(), "long");
    name.put(ShortFormat.get(), "short");
    name.put(StringFormat.get(), "string");

    name.put(DescendingBigDecimalFormat.get(), "descendingbigdecimal");
    name.put(DescendingDoubleFormat.get(), "descendingdouble");
    name.put(DescendingFloatFormat.get(), "descendingfloat");
    name.put(DescendingIntFormat.get(),  "descendingint");
    name.put(DescendingLongFormat.get(), "descendinglong");
    name.put(DescendingShortFormat.get(), "descendingshort");
    name.put(DescendingStringFormat.get(), "descendingstring");
    toName = name;
  }

  public static DataFormat getFormat(String name) {
    return toFormatFromName.get(name);
  }

  public static String getName(DataFormat format) {
    return toName.get(format);
  }
}
