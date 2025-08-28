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

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryCustomOrderBy;

public class QueryCustomOrderByAdapter extends AbstractSemossTypeAdapter<QueryCustomOrderBy>
    implements IQuerySortAdapterHelper {

  @Override
  public QueryCustomOrderBy read(JsonReader in) throws IOException {
    if (in.peek() == JsonToken.NULL) {
      in.nextNull();
      return null;
    }

    // remove the beginning objects
    in.beginObject();
    in.nextName();
    in.nextString();
    in.nextName();

    // now we read the actual content
    QueryCustomOrderBy value = readContent(in);
    in.endObject();
    return value;
  }

  @Override
  public QueryCustomOrderBy readContent(JsonReader in) throws IOException {
    QueryCustomOrderBy value = new QueryCustomOrderBy();

    in.beginObject();
    while (in.hasNext()) {
      String key = in.nextName();
      if (key.equals("columnToSort")) {
        IQuerySelectorAdapter selectorAdapter = new IQuerySelectorAdapter();
        IQuerySelector selector = selectorAdapter.read(in);
        value.setColumnToSort((QueryColumnSelector) selector);
      } else if (key.equals("values")) {
        List<Object> customOrder = new Vector<>();
        in.beginArray();
        while (in.hasNext()) {
          JsonToken type = in.peek();
          if (type == JsonToken.NULL) {
            customOrder.add(null);
            in.nextNull();
          } else if (type == JsonToken.NUMBER) {
            // assume integer
            try {
              int obj = in.nextInt();
              customOrder.add(obj);
            } catch (Exception e) {
              // ignore
              double obj = in.nextDouble();
              customOrder.add(obj);
            }
          } else {
            // should be a string
            customOrder.add(in.nextString());
          }
        }
        in.endArray();
        value.setCustomOrder(customOrder);
      }
    }
    in.endObject();
    return value;
  }

  @Override
  public void write(JsonWriter out, QueryCustomOrderBy value) throws IOException {
    if (value == null) {
      out.nullValue();
      return;
    }

    // always start with the type of the query selector
    out.beginObject();
    out.name("type").value(IQuerySort.QUERY_SORT_TYPE.CUSTOM.toString());
    out.name("content");
    // content object
    out.beginObject();
    out.name("columnToSort");
    // generic selector output
    IQuerySelector s = value.getColumnToSort();
    TypeAdapter adapter = IQuerySelector.getAdapterForSelector(s.getSelectorType());
    adapter.write(out, s);
    out.name("values");
    List<Object> customOrder = value.getCustomOrder();
    out.beginArray();
    for (Object obj : customOrder) {
      if (obj == null) {
        out.nullValue();
      } else if (obj instanceof Integer) {
        out.value((Integer) obj);
      } else if (obj instanceof Double) {
        out.value((Double) obj);
      } else {
        out.value(obj.toString());
      }
    }
    out.endArray();
    out.endObject();
    out.endObject();
  }
}
