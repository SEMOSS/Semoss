package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.om.Pixel;
import prerna.om.PixelList;

public class PixelListAdapter  extends TypeAdapter<PixelList> {
	
	@Override
	public PixelList read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		PixelList pixelList = new PixelList();

		PixelAdapter adapter = new PixelAdapter();
		in.beginArray();
		while(in.hasNext()) {
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
		for(int i = 0; i < value.size(); i++) {
			Pixel p = value.get(i);
			adapter.write(out, p);
		}
		out.endArray();
	}
	
}
