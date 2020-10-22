package prerna.util.gson;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.om.Pixel;

public class PixelAdapter  extends TypeAdapter<Pixel> {
	
	private static final Gson GSON = GsonUtility.getDefaultGson();

	@Override
	public Pixel read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		String id = null;
		String pixelString = null;
		Map<String, Map<String, Object>> startingFrameHeaders = null;
		Map<String, Map<String, Object>> endingFrameHeaders = null;
		Map<String, List<Map>> reactorInput = null;
		
		TypeAdapter mapAdapter = GSON.getAdapter(Map.class);

		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(in.peek() == JsonToken.NULL) {
				in.nextNull();
				continue;
			}
			
			if(key.equals("id")) {
				id = in.nextString();
			} else if(key.equals("pixelString")) {
				pixelString = in.nextString();
			} else if(key.equals("startingFrameHeaders")) {
				startingFrameHeaders = (Map<String, Map<String, Object>>) mapAdapter.read(in);
			} else if(key.equals("endingFrameHeaders")) {
				endingFrameHeaders = (Map<String, Map<String, Object>>) mapAdapter.read(in);
			} else if(key.equals("reactorInput")) {
				reactorInput = (Map<String, List<Map>>) mapAdapter.read(in);
			}
		}
		in.endObject();

		Pixel pixel = new Pixel(id, pixelString);
		pixel.setStartingFrameHeaders(startingFrameHeaders);
		pixel.setEndingFrameHeaders(endingFrameHeaders);
		pixel.setReactorInput(reactorInput);
		return pixel;
	}
	
	@Override
	public void write(JsonWriter out, Pixel value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		out.beginObject();
		out.name("id");
		out.value(value.getId());
		out.name("pixelString");
		out.value(value.getPixelString());
		out.name("startingFrameHeaders");
		if(value.getStartingFrameHeaders() != null) {
			TypeAdapter adapter = GSON.getAdapter(value.getStartingFrameHeaders().getClass());
			adapter.write(out, value.getStartingFrameHeaders());
		} else {
			out.nullValue();
		}
		out.name("endingFrameHeaders");
		if(value.getEndingFrameHeaders() != null) {
			TypeAdapter adapter = GSON.getAdapter(value.getEndingFrameHeaders().getClass());
			adapter.write(out, value.getEndingFrameHeaders());
		} else {
			out.nullValue();
		}
		out.name("reactorInput");
		if(value.getReactorInput() != null) {
			TypeAdapter adapter = GSON.getAdapter(value.getReactorInput().getClass());
			adapter.write(out, value.getReactorInput());
		} else {
			out.nullValue();
		}
		
		out.endObject();
	}
	
}
