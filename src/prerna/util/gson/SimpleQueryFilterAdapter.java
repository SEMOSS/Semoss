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
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SimpleQueryFilterAdapter extends AbstractSemossTypeAdapter<SimpleQueryFilter>
		implements
			IQueryFilterAdapterHelper {

	@Override
	public SimpleQueryFilter read(JsonReader in) throws IOException {
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
		SimpleQueryFilter value = readContent(in);
		in.endObject();
		return value;
	}

	@Override
	public SimpleQueryFilter readContent(JsonReader in) throws IOException {
		NounMetadataAdapter adapter = new NounMetadataAdapter();

		NounMetadata left = null;
		NounMetadata right = null;
		String comparator = null;

		in.beginObject();
		while (in.hasNext()) {
			String name = in.nextName();
			if (name.equals("left")) {
				left = adapter.read(in);
			} else if (name.equals("comparator")) {
				comparator = in.nextString();
			} else if (name.equals("right")) {
				right = adapter.read(in);
			}
		}
		in.endObject();

		return new SimpleQueryFilter(left, comparator, right);
	}

	@Override
	public void write(JsonWriter out, SimpleQueryFilter value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		NounMetadata left = value.getLComparison();
		NounMetadata right = value.getRComparison();
		String comp = value.getComparator();

		NounMetadataAdapter adapter = new NounMetadataAdapter();

		out.beginObject();
		out.name("type").value(IQueryFilter.QUERY_FILTER_TYPE.SIMPLE.toString());
		out.name("content");
		// content object
		out.beginObject();
		out.name("left");
		adapter.write(out, left);
		out.name("comparator").value(comp);
		out.name("right");
		adapter.write(out, right);
		out.endObject();
		out.endObject();
	}
}
