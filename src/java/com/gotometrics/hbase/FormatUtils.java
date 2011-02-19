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

package com.gotometrics.format;

import com.gotometrics.thrift.generated.Constants;

import java.util.HashMap;
import java.util.Map;

public class FormatUtils
{
  private static final Map<Byte,DataFormat> toFormatFromId;
  private static final Map<String,DataFormat> toFormatFromName;
  private static final Map<DataFormat, Byte> toId;
  private static final Map<DataFormat, String> toName;

  static {
    Map<Byte,DataFormat> fmtId = 
      new HashMap<Byte, DataFormat>(Constants.NUM_FORMATS);
    fmtId.put(Constants.BYTEARRAY_FORMAT, ByteArrayFormat.get());
    fmtId.put(Constants.BIGDECIMAL_FORMAT, BigDecimalFormat.get());
    fmtId.put(Constants.DATE_FORMAT, DateFormat.get());
    fmtId.put(Constants.DOUBLE_FORMAT, DoubleFormat.get());
    fmtId.put(Constants.FLOAT_FORMAT, FloatFormat.get());
    fmtId.put(Constants.INT_FORMAT, IntFormat.get());
    fmtId.put(Constants.LONG_FORMAT, LongFormat.get());
    fmtId.put(Constants.SHORT_FORMAT, ShortFormat.get());
    fmtId.put(Constants.STRING_FORMAT, StringFormat.get());
    fmtId.put(Constants.TIME_FORMAT, TimeFormat.get());
    fmtId.put(Constants.TIMESTAMP_FORMAT, TimestampFormat.get());

    fmtId.put(Constants.DESCENDING_BYTEARRAY_FORMAT, 
        DescendingByteArrayFormat.get());
    fmtId.put(Constants.DESCENDING_BIGDECIMAL_FORMAT, 
        DescendingBigDecimalFormat.get());
    fmtId.put(Constants.DESCENDING_DATE_FORMAT, DescendingDateFormat.get());
    fmtId.put(Constants.DESCENDING_DOUBLE_FORMAT, DescendingDoubleFormat.get());
    fmtId.put(Constants.DESCENDING_FLOAT_FORMAT, DescendingFloatFormat.get());
    fmtId.put(Constants.DESCENDING_INT_FORMAT, DescendingIntFormat.get());
    fmtId.put(Constants.DESCENDING_LONG_FORMAT, DescendingLongFormat.get());
    fmtId.put(Constants.DESCENDING_SHORT_FORMAT, DescendingShortFormat.get());
    fmtId.put(Constants.DESCENDING_STRING_FORMAT, DescendingStringFormat.get());
    fmtId.put(Constants.DESCENDING_TIME_FORMAT, DescendingTimeFormat.get());
    fmtId.put(Constants.DESCENDING_TIMESTAMP_FORMAT, 
            DescendingTimestampFormat.get());
    toFormatFromId = fmtId;

    Map<String,DataFormat> fmtName = 
      new HashMap<String, DataFormat>(Constants.NUM_FORMATS);
    fmtName.put("bytearray", ByteArrayFormat.get());
    fmtName.put("bigdecimal", BigDecimalFormat.get());
    fmtName.put("date", DateFormat.get());
    fmtName.put("double", DoubleFormat.get());
    fmtName.put("float", FloatFormat.get());
    fmtName.put("int", IntFormat.get());
    fmtName.put("long", LongFormat.get());
    fmtName.put("short", ShortFormat.get());
    fmtName.put("string", StringFormat.get());
    fmtName.put("time", TimeFormat.get());
    fmtName.put("timestamp", TimestampFormat.get());

    fmtName.put("descendingbytearray", DescendingByteArrayFormat.get());
    fmtName.put("descendingbigdecimal", DescendingBigDecimalFormat.get());
    fmtName.put("descendingdate", DescendingDateFormat.get());
    fmtName.put("descendingdouble", DescendingDoubleFormat.get());
    fmtName.put("descendingfloat", DescendingFloatFormat.get());
    fmtName.put("descendingint", DescendingIntFormat.get());
    fmtName.put("descendinglong", DescendingLongFormat.get());
    fmtName.put("descendingshort", DescendingShortFormat.get());
    fmtName.put("descendingstring", DescendingStringFormat.get());
    fmtName.put("descendingtime", DescendingTimeFormat.get());
    fmtName.put("descendingtimestamp", DescendingTimestampFormat.get());
    toFormatFromName = fmtName;

    Map<DataFormat, Byte> id = 
      new HashMap<DataFormat, Byte>(Constants.NUM_FORMATS);
    id.put(ByteArrayFormat.get(), Constants.BYTEARRAY_FORMAT);
    id.put(BigDecimalFormat.get(), Constants.BIGDECIMAL_FORMAT);
    id.put(DateFormat.get(), Constants.DATE_FORMAT);
    id.put(DoubleFormat.get(), Constants.DOUBLE_FORMAT);
    id.put(FloatFormat.get(), Constants.FLOAT_FORMAT);
    id.put(IntFormat.get(), Constants.INT_FORMAT);
    id.put(LongFormat.get(), Constants.LONG_FORMAT);
    id.put(ShortFormat.get(), Constants.SHORT_FORMAT);
    id.put(StringFormat.get(), Constants.STRING_FORMAT);
    id.put(TimeFormat.get(), Constants.TIME_FORMAT);
    id.put(TimestampFormat.get(), Constants.TIMESTAMP_FORMAT);

    id.put(DescendingByteArrayFormat.get(), 
        Constants.DESCENDING_BYTEARRAY_FORMAT);
    id.put(DescendingBigDecimalFormat.get(), 
        Constants.DESCENDING_BIGDECIMAL_FORMAT);
    id.put(DescendingDateFormat.get(), Constants.DESCENDING_DATE_FORMAT);
    id.put(DescendingDoubleFormat.get(), Constants.DESCENDING_DOUBLE_FORMAT);
    id.put(DescendingFloatFormat.get(), Constants.DESCENDING_FLOAT_FORMAT);
    id.put(DescendingIntFormat.get(), Constants.DESCENDING_INT_FORMAT);
    id.put(DescendingLongFormat.get(), Constants.DESCENDING_LONG_FORMAT);
    id.put(DescendingShortFormat.get(), Constants.DESCENDING_SHORT_FORMAT);
    id.put(DescendingStringFormat.get(), Constants.DESCENDING_STRING_FORMAT);
    id.put(DescendingTimeFormat.get(), Constants.DESCENDING_TIME_FORMAT);
    id.put(DescendingTimestampFormat.get(), 
              Constants.DESCENDING_TIMESTAMP_FORMAT);
    toId = id;

    Map<DataFormat, String> name = 
      new HashMap<DataFormat, String>(Constants.NUM_FORMATS);

    name.put(ByteArrayFormat.get(), "bytearray");
    name.put(BigDecimalFormat.get(), "bigdecimal");
    name.put(DateFormat.get(), "date");
    name.put(DoubleFormat.get(), "double");
    name.put(FloatFormat.get(), "float");
    name.put(IntFormat.get(), "int");
    name.put(LongFormat.get(), "long");
    name.put(ShortFormat.get(), "short");
    name.put(StringFormat.get(), "string");
    name.put(TimeFormat.get(), "time");
    name.put(TimestampFormat.get(), "timestamp");

    name.put(DescendingByteArrayFormat.get(), "descendingbytearray");
    name.put(DescendingBigDecimalFormat.get(), "descendingbigdecimal");
    name.put(DescendingDateFormat.get(), "descendingdate");
    name.put(DescendingDoubleFormat.get(), "descendingdouble");
    name.put(DescendingFloatFormat.get(), "descendingfloat");
    name.put(DescendingIntFormat.get(),  "descendingint");
    name.put(DescendingLongFormat.get(), "descendinglong");
    name.put(DescendingShortFormat.get(), "descendingshort");
    name.put(DescendingStringFormat.get(), "descendingstring");
    name.put(DescendingTimeFormat.get(),  "descendingtime");
    name.put(DescendingTimestampFormat.get(), "descendingtimestamp");
    toName = name;
  }

  public static DataFormat getFormat(byte id) {
    return toFormatFromId.get(id);
  }

  public static DataFormat getFormat(String name) {
    return toFormatFromName.get(name);
  }

  public static byte getId(DataFormat format) {
    return toId.get(format);
  }

  public static String getName(DataFormat format) {
    return toName.get(format);
  }
}
