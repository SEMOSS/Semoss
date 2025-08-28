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
import prerna.query.querystruct.selectors.IQuerySelector;

public class IQuerySelectorAdapter extends AbstractSemossTypeAdapter<IQuerySelector> {

	@Override
	public IQuerySelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		// should start with the type
		in.beginObject();
		in.nextName();
		String selectorTypeString = in.nextString();

		// get the correct adapter
		IQuerySelector.SELECTOR_TYPE selectorType = IQuerySelector.convertStringToSelectorType(selectorTypeString);
		AbstractSemossTypeAdapter reader = IQuerySelector.getAdapterForSelector(selectorType);
		reader.setInsight(this.insight);

		// now we should have the content object
		in.nextName();
		IQuerySelector selector = ((IQuerySelectorAdapterHelper) reader).readContent(in);
		in.endObject();

		return selector;
	}

	@Override
	public void write(JsonWriter out, IQuerySelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		// go to the specific instance impl to write it
		IQuerySelector.SELECTOR_TYPE selectorType = value.getSelectorType();
		TypeAdapter reader = IQuerySelector.getAdapterForSelector(selectorType);
		reader.write(out, value);
	}
}
