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
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;

public class AndQueryFilterAdapter extends AbstractSemossTypeAdapter<AndQueryFilter>
    implements IQueryFilterAdapterHelper {

  @Override
  public AndQueryFilter read(JsonReader in) throws IOException {
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
    AndQueryFilter value = readContent(in);
    in.endObject();
    return value;
  }

  @Override
  public AndQueryFilter readContent(JsonReader in) throws IOException {
    AndQueryFilter filters = new AndQueryFilter();
    in.beginArray();
    while (in.hasNext()) {
      IQueryFilterAdapter adapter = new IQueryFilterAdapter();
      IQueryFilter f = adapter.read(in);
      filters.addFilter(f);
    }
    in.endArray();

    return filters;
  }

  @Override
  public void write(JsonWriter out, AndQueryFilter value) throws IOException {
    if (value == null) {
      out.nullValue();
      return;
    }

    out.beginObject();
    out.name("type").value(IQueryFilter.QUERY_FILTER_TYPE.AND.toString());
    out.name("content");
    out.beginArray();
    List<IQueryFilter> filters = value.getFilterList();
    for (IQueryFilter f : filters) {
      TypeAdapter adapter = IQueryFilter.getAdapterForFilter(f.getQueryFilterType());
      adapter.write(out, f);
    }
    out.endArray();
    out.endObject();
  }
}
