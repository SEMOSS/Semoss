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

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import prerna.query.querystruct.selectors.IQuerySelector;

public interface IQuerySelectorAdapterHelper {

  IQuerySelector readContent(JsonReader in) throws IOException;

  public static void writeStringMap(JsonWriter out, Map<String, String> map) throws IOException {
    if (map == null) {
      out.nullValue();
      return;
    }

    out.beginObject();
    for (String key : map.keySet()) {
      out.name(key);
      out.value(map.get(key));
    }
    out.endObject();
  }

  public static Map<String, String> readStringMap(JsonReader in) throws IOException {
    if (in.peek() == JsonToken.NULL) {
      return null;
    }

    Map<String, String> values = new HashMap<String, String>();

    in.beginObject();
    while (in.hasNext()) {
      String key = in.nextName();
      String value = in.nextString();
      values.put(key, value);
    }
    in.endObject();

    return values;
  }
}
