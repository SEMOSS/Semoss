/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util.gson;

import static prerna.util.gson.IHeadersDataRowAdapter.deserializeValues;
import static prerna.util.gson.IHeadersDataRowAdapter.serializeValues;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import prerna.om.HeadersDataRow;
import prerna.util.gson.IHeadersDataRowAdapter.SerializedValuesAndTypes;

public class HeadersDataRowAdapter extends AbstractSemossTypeAdapter<HeadersDataRow> {

  @Override
  public HeadersDataRow read(JsonReader in) throws IOException {
    if (in.peek() == JsonToken.NULL) {
      in.nextNull();
      return null;
    }

    List<String> headers = new ArrayList<>();
    List<String> rawHeaders = new ArrayList<>();
    List<String> values = new ArrayList<>();
    List<String> valueTypes = new ArrayList<>();
    List<String> rawValues = new ArrayList<>();
    List<String> rawValueTypes = new ArrayList<>();

    if (in.peek() != JsonToken.NAME || !in.nextName().equals("row")) {
      throw new IllegalArgumentException(
          "Header is not serialized properly. Must contain row object.");
    }
    in.beginObject();

    while (in.hasNext()) {
      if (in.peek() == JsonToken.NAME) {
        String key = in.nextName();
        if (key.equals("headers")) {
          in.beginArray();
          while (in.hasNext()) {
            headers.add(in.nextString());
          }
          in.endArray();
        } else if (key.equals("rawHeaders")) {
          in.beginArray();
          while (in.hasNext()) {
            rawHeaders.add(in.nextString());
          }
          in.endArray();
        } else if (key.equals("values")) {
          in.beginArray();
          while (in.hasNext()) {
            if (in.peek() == JsonToken.NULL) {
              values.add(null);
              in.nextNull();
            } else {
              values.add(in.nextString());
            }
          }
          in.endArray();
        } else if (key.equals("valueTypes")) {
          in.beginArray();
          while (in.hasNext()) {
            valueTypes.add(in.nextString());
          }
          in.endArray();
        } else if (key.equals("rawValues")) {
          in.beginArray();
          while (in.hasNext()) {
            if (in.peek() == JsonToken.NULL) {
              rawValues.add(null);
              in.nextNull();
            } else {
              rawValues.add(in.nextString());
            }
          }
          in.endArray();
        } else if (key.equals("rawValueTypes")) {
          in.beginArray();
          while (in.hasNext()) {
            rawValueTypes.add(in.nextString());
          }
          in.endArray();
        }
      }
    }
    in.endObject();

    Object[] deserializedValues =
        deserializeValues(values.toArray(new String[0]), valueTypes.toArray(new String[0]));
    Object[] deserializedRawValues =
        deserializeValues(rawValues.toArray(new String[0]), rawValueTypes.toArray(new String[0]));
    HeadersDataRow value =
        new HeadersDataRow(
            headers.toArray(new String[0]),
            rawHeaders.toArray(new String[0]),
            deserializedValues,
            deserializedRawValues);
    return value;
  }

  @Override
  public void write(JsonWriter out, HeadersDataRow value) throws IOException {
    if (value == null) {
      out.nullValue();
      return;
    }

    out.beginObject();

    // always start with the type of the header
    out.name("type").value(value.getHeaderType().toString());

    out.name("row").beginObject();

    // headers
    out.name("headers");
    out.beginArray();
    for (String header : value.getHeaders()) {
      out.value(header);
    }
    out.endArray();

    // rawHeaders
    out.name("rawHeaders");
    out.beginArray();
    for (String rawHeader : value.getRawHeaders()) {
      out.value(rawHeader);
    }
    out.endArray();

    // values
    SerializedValuesAndTypes result = serializeValues(value.getValues());
    String[] serializedValues = result.getSerializedValues();
    String[] serializedValueTypes = result.getSerializedValueTypes();

    out.name("values");
    out.beginArray();
    for (String serializedValue : serializedValues) {
      out.value(serializedValue);
    }
    out.endArray();

    out.name("valueTypes");
    out.beginArray();
    for (String serializedValueType : serializedValueTypes) {
      out.value(serializedValueType);
    }
    out.endArray();

    // rawValues
    SerializedValuesAndTypes rawResult = serializeValues(value.getRawValues());
    String[] rawSerializedValues = rawResult.getSerializedValues();
    String[] rawSerializedValueTypes = rawResult.getSerializedValueTypes();

    out.name("rawValues");
    out.beginArray();
    for (String rawSerializedValue : rawSerializedValues) {
      out.value(rawSerializedValue);
    }
    out.endArray();

    out.name("rawValueTypes");
    out.beginArray();
    for (String rawSerializedValueType : rawSerializedValueTypes) {
      out.value(rawSerializedValueType);
    }
    out.endArray();

    out.endObject();

    out.endObject();
  }
}
