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
import prerna.query.querystruct.filters.IQueryFilter;

public class IQueryFilterAdapter extends AbstractSemossTypeAdapter<IQueryFilter> {

	@Override
	public IQueryFilter read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		// should start with the type
		in.beginObject();
		in.nextName();
		String filterTypeString = in.nextString();

		// get the correct adapter
		IQueryFilter.QUERY_FILTER_TYPE filterType = IQueryFilter.convertStringToFilterType(filterTypeString);
		TypeAdapter reader = IQueryFilter.getAdapterForFilter(filterType);
		if (reader instanceof IQueryFilterAdapterHelper) {
			// now we should have the content object
			in.nextName();
			IQueryFilter filter = ((IQueryFilterAdapterHelper) reader).readContent(in);
			in.endObject();

			return filter;
		} else {
			// this is the case of a subquery as a filter
			return (IQueryFilter) reader.read(in);
		}
	}

	@Override
	public void write(JsonWriter out, IQueryFilter value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		// go to the specific instance impl to write it
		IQueryFilter.QUERY_FILTER_TYPE filterType = value.getQueryFilterType();
		TypeAdapter adapter = IQueryFilter.getAdapterForFilter(filterType);
		adapter.write(out, value);
	}
}
