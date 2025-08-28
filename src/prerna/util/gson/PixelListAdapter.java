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
import prerna.om.Pixel;
import prerna.om.PixelList;

public class PixelListAdapter extends AbstractSemossTypeAdapter<PixelList> {

	@Override
	public PixelList read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		PixelList pixelList = new PixelList();

		PixelAdapter adapter = new PixelAdapter();
		in.beginArray();
		while (in.hasNext()) {
			Pixel pixel = adapter.read(in);
			pixelList.addPixel(pixel);
		}
		in.endArray();

		return pixelList;
	}

	@Override
	public void write(JsonWriter out, PixelList value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		PixelAdapter adapter = new PixelAdapter();
		out.beginArray();
		int size = value.size();
		for (int i = 0; i < size; i++) {
			Pixel p = value.get(i);
			adapter.write(out, p);
		}
		out.endArray();
	}
}
