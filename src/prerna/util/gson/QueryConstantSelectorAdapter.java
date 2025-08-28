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

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.reactor.qs.SubQueryExpression;

public class QueryConstantSelectorAdapter extends AbstractSemossTypeAdapter<QueryConstantSelector>
		implements
			IQuerySelectorAdapterHelper {

	@Override
	public QueryConstantSelector read(JsonReader in) throws IOException {
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
		QueryConstantSelector value = readContent(in);
		in.endObject();
		return value;
	}

	@Override
	public QueryConstantSelector readContent(JsonReader in) throws IOException {
		QueryConstantSelector value = new QueryConstantSelector();
		in.beginObject();
		while (in.hasNext()) {
			String key = in.nextName();
			if (key.equals("alias")) {
				value.setAlias(in.nextString());
			} else if (key.equals("constant")) {
				if (in.peek() == JsonToken.NULL) {
					value.setConstant(null);
				} else if (in.peek() == JsonToken.NUMBER) {
					value.setConstant(in.nextDouble());
				} else if (in.peek() == JsonToken.BOOLEAN) {
					value.setConstant(in.nextBoolean());
				} else if (in.peek() == JsonToken.STRING) {
					value.setConstant(in.nextString());
				} else if (in.peek() == JsonToken.BEGIN_OBJECT) {
					SubQueryExpressionAdapter adapter = new SubQueryExpressionAdapter();
					SubQueryExpression obj = adapter.read(in);
					obj.setInsight(this.insight);
					value.setConstant(obj);
				}
			}
		}
		in.endObject();
		return value;
	}

	@Override
	public void write(JsonWriter out, QueryConstantSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		// always start with the type of the query selector
		out.beginObject();
		out.name("type").value(IQuerySelector.SELECTOR_TYPE.CONSTANT.toString());
		out.name("content");
		// content object
		out.beginObject();
		out.name("alias").value(value.getAlias());
		Object val = value.getConstant();
		if (val == null) {
			out.name("constant").nullValue();
		} else if (val instanceof Number) {
			out.name("constant").value((Number) val);
		} else if (val instanceof Boolean) {
			out.name("constant").value((Boolean) val);
		} else if (val instanceof SubQueryExpression) {
			out.name("constant");
			SubQueryExpressionAdapter adapter = new SubQueryExpressionAdapter();
			adapter.write(out, (SubQueryExpression) val);
		} else {
			out.name("constant").value(val.toString());
		}
		out.endObject();
		out.endObject();
	}
}
