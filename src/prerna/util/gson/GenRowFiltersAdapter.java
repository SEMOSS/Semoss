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
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;

public class GenRowFiltersAdapter extends AbstractSemossTypeAdapter<GenRowFilters> {

	@Override
	public GenRowFilters read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		GenRowFilters grf = new GenRowFilters();
		in.beginArray();
		while (in.hasNext()) {
			IQueryFilterAdapter filterAdapter = new IQueryFilterAdapter();
			IQueryFilter filter = filterAdapter.read(in);
			grf.addFilters(filter);
		}
		in.endArray();

		return grf;
	}

	@Override
	public void write(JsonWriter out, GenRowFilters value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		out.beginArray();
		List<IQueryFilter> filters = value.getFilters();
		int numFilters = filters.size();
		for (int i = 0; i < numFilters; i++) {
			IQueryFilter f = filters.get(i);
			TypeAdapter adapter = IQueryFilter.getAdapterForFilter(f.getQueryFilterType());
			adapter.write(out, f);
		}
		out.endArray();
	}
}
