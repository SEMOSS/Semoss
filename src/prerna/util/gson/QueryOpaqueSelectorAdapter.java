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
import prerna.query.querystruct.selectors.QueryOpaqueSelector;

public class QueryOpaqueSelectorAdapter extends AbstractSemossTypeAdapter<QueryOpaqueSelector>
		implements
			IQuerySelectorAdapterHelper {

	@Override
	public QueryOpaqueSelector read(JsonReader in) throws IOException {
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
		QueryOpaqueSelector value = readContent(in);
		in.endObject();
		return value;
	}

	@Override
	public QueryOpaqueSelector readContent(JsonReader in) throws IOException {
		QueryOpaqueSelector value = new QueryOpaqueSelector();
		in.beginObject();
		while (in.hasNext()) {
			String key = in.nextName();
			if (key.equals("table")) {
				value.setTable(in.nextString());
			} else if (key.equals("alias")) {
				value.setAlias(in.nextString());
			} else if (key.equals("querySyntax")) {
				value.setQuerySelectorSyntax(in.nextString());
			}
		}
		in.endObject();
		return value;
	}

	@Override
	public void write(JsonWriter out, QueryOpaqueSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		// always start with the type of the query selector
		out.beginObject();
		out.name("type").value(IQuerySelector.SELECTOR_TYPE.OPAQUE.toString());
		out.name("content");
		// content object
		out.beginObject();
		out.name("alias").value(value.getAlias());
		out.name("table").value(value.getTable());
		out.name("querySyntax").value(value.getQuerySelectorSyntax());
		out.endObject();
		out.endObject();
	}
}
