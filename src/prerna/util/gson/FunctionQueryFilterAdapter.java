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
import prerna.query.querystruct.filters.FunctionQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class FunctionQueryFilterAdapter extends AbstractSemossTypeAdapter<FunctionQueryFilter> {

  @Override
  public FunctionQueryFilter read(JsonReader in) throws IOException {
    if (in.peek() == JsonToken.NULL) {
      in.nextNull();
      return null;
    }

    // might start with the type of the filter
    if (in.peek() == JsonToken.STRING) {
      in.nextString();
    }

    QueryFunctionSelectorAdapter adapter = new QueryFunctionSelectorAdapter();
    QueryFunctionSelector selector = null;
    in.beginObject();
    while (in.hasNext()) {
      // there is only 1 key
      in.nextName();
      selector = adapter.read(in);
    }
    in.endObject();

    FunctionQueryFilter filter = new FunctionQueryFilter();
    filter.setFunctionSelector(selector);
    return filter;
  }

  @Override
  public void write(JsonWriter out, FunctionQueryFilter value) throws IOException {
    if (value == null) {
      out.nullValue();
      return;
    }

    out.value(IQueryFilter.QUERY_FILTER_TYPE.FUNCTION.toString());

    QueryFunctionSelectorAdapter adapter = new QueryFunctionSelectorAdapter();

    out.beginObject();
    out.name("selector");
    adapter.write(out, value.getFunctionSelector());
    out.endObject();
  }
}
