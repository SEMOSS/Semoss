package prerna.util.gson;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;

public interface IQuerySelectorAdapterHelper {

	IQuerySelector readContent(JsonReader in) throws IOException;
	
	public static void writeStringMap(JsonWriter out, Map<String, String> map) throws IOException {
		if(map == null) {
			out.nullValue();
			return;
		}
		
		out.beginObject();
		for(String key : map.keySet()) {
			out.name(key);
			out.value(map.get(key));
		}
		out.endObject();
	}
	
	public static Map<String, String> readStringMap(JsonReader in) throws IOException {
		if(in.peek() == JsonToken.NULL) {
			return null;
		}
		
		Map<String, String> values = new HashMap<String, String>();
		
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			String value = in.nextString();
			values.put(key, value);
		}
		in.endObject();
		
		return values;
	}
}
