/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.util.gson;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.List;
import java.util.Vector;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class QueryFunctionSelectorAdapter extends AbstractSemossTypeAdapter<QueryFunctionSelector>
    implements IQuerySelectorAdapterHelper {

  @Override
  public QueryFunctionSelector read(JsonReader in) throws IOException {
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
    QueryFunctionSelector value = readContent(in);
    in.endObject();
    return value;
  }

  @Override
  public QueryFunctionSelector readContent(JsonReader in) throws IOException {
    QueryFunctionSelector value = new QueryFunctionSelector();
    in.beginObject();
    while (in.hasNext()) {
      String key = in.nextName();
      if (key.equals("alias")) {
        value.setAlias(in.nextString());
      } else if (key.equals("distinct")) {
        value.setDistinct(in.nextBoolean());
      } else if (key.equals("colCast")) {
        value.setColCast(in.nextString());
      } else if (key.equals("function")) {
        value.setFunction(in.nextString());
      } else if (key.equals("dataType")) {
        value.setDataType(in.nextString());

      } else if (key.equals("innerSelectors")) {
        List<IQuerySelector> innerList = new Vector<IQuerySelector>();

        in.beginArray();
        while (in.hasNext()) {
          IQuerySelectorAdapter innerAdapter = new IQuerySelectorAdapter();
          innerAdapter.setInsight(this.insight);
          IQuerySelector innerSelector = innerAdapter.read(in);
          innerList.add(innerSelector);
        }
        in.endArray();

        value.setInnerSelector(innerList);
      }
    }
    in.endObject();
    return value;
  }

  @Override
  public void write(JsonWriter out, QueryFunctionSelector value) throws IOException {
    if (value == null) {
      out.nullValue();
      return;
    }

    // always start with the type of the query selector
    out.beginObject();
    out.name("type").value(IQuerySelector.SELECTOR_TYPE.FUNCTION.toString());
    out.name("content");

    // content object
    out.beginObject();
    out.name("alias").value(value.getAlias());
    out.name("function").value(value.getFunction());
    out.name("distinct").value(value.isDistinct());
    out.name("colCast").value(value.getColCast());
    out.name("dataType").value(value.getDataType());

    out.name("innerSelectors");
    out.beginArray();
    List<IQuerySelector> innerList = value.getInnerSelector();
    for (IQuerySelector inner : innerList) {
      TypeAdapter leftOutput = IQuerySelector.getAdapterForSelector(inner.getSelectorType());
      leftOutput.write(out, inner);
    }
    out.endArray();
    out.endObject();

    out.endObject();
  }
}
