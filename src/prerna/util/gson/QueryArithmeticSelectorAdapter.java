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
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;

public class QueryArithmeticSelectorAdapter
    extends AbstractSemossTypeAdapter<QueryArithmeticSelector>
    implements IQuerySelectorAdapterHelper {

  @Override
  public QueryArithmeticSelector read(JsonReader in) throws IOException {
    if (in.peek() == JsonToken.NULL) {
      in.nextNull();
      return null;
    }

    // remove the beginning objects
    in.beginObject();
    in.nextName();
    in.nextString();
    in.nextName();
    QueryArithmeticSelector value = readContent(in);
    in.endObject();
    return value;
  }

  @Override
  public QueryArithmeticSelector readContent(JsonReader in) throws IOException {
    QueryArithmeticSelector value = new QueryArithmeticSelector();
    in.beginObject();
    while (in.hasNext()) {
      String key = in.nextName();
      if (key.equals("alias")) {
        value.setAlias(in.nextString());
      } else if (key.equals("mathExpr")) {
        value.setMathExpr(in.nextString());
      } else if (key.equals("left")) {
        in.beginArray();
        while (in.hasNext()) {
          if (in.peek() == JsonToken.STRING) {
            // this is the type of the left selector
            in.nextString();
          } else if (in.peek() == JsonToken.BEGIN_OBJECT) {
            IQuerySelectorAdapter leftAdapter = new IQuerySelectorAdapter();
            leftAdapter.setInsight(this.insight);
            IQuerySelector leftSelector = leftAdapter.read(in);
            value.setLeftSelector(leftSelector);
          }
        }
        in.endArray();
      } else if (key.equals("right")) {
        in.beginArray();
        while (in.hasNext()) {
          if (in.peek() == JsonToken.STRING) {
            // this is the type of the left selector
            in.nextString();
          } else if (in.peek() == JsonToken.BEGIN_OBJECT) {
            IQuerySelectorAdapter rightAdapter = new IQuerySelectorAdapter();
            rightAdapter.setInsight(this.insight);
            IQuerySelector rightSelector = rightAdapter.read(in);
            value.setRightSelector(rightSelector);
          }
        }
        in.endArray();
      }
    }
    in.endObject();
    return value;
  }

  @Override
  public void write(JsonWriter out, QueryArithmeticSelector value) throws IOException {
    if (value == null) {
      out.nullValue();
      return;
    }

    // always start with the type of the query selector
    out.beginObject();
    out.name("type").value(IQuerySelector.SELECTOR_TYPE.ARITHMETIC.toString());
    out.name("content");

    // content object
    out.beginObject();
    out.name("alias").value(value.getAlias());
    out.name("mathExpr").value(value.getMathExpr());

    out.name("left");
    out.beginArray();
    IQuerySelector left = value.getLeftSelector();
    TypeAdapter leftOutput = IQuerySelector.getAdapterForSelector(left.getSelectorType());
    leftOutput.write(out, left);
    out.endArray();

    out.name("right");
    out.beginArray();
    IQuerySelector right = value.getRightSelector();
    TypeAdapter rightOutput = IQuerySelector.getAdapterForSelector(right.getSelectorType());
    rightOutput.write(out, right);
    out.endArray();
    out.endObject();

    out.endObject();
  }
}
