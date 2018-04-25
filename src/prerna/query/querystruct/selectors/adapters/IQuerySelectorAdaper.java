package prerna.query.querystruct.selectors.adapters;

import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;

public class IQuerySelectorAdaper extends TypeAdapter<IQuerySelector> {

	private static final Gson GSON = new Gson();
	
	@Override
	public void write(JsonWriter out, IQuerySelector value) throws IOException {
		// get the instance level gson
		Gson gson = IQuerySelector.getGson();
		out.value(gson.toJson(value));
	}

	@Override
	public IQuerySelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		String mapStr = in.nextString();
		Map<String, Object> map = GSON.fromJson(mapStr, Map.class);
		String selectorTypeString = map.get("selectorType") + "";

		SELECTOR_TYPE s = IQuerySelector.convertStringToSelectorType(selectorTypeString);
		return (IQuerySelector) IQuerySelector.getGson().fromJson(mapStr.toString(), IQuerySelector.getQuerySelectorClassFromType(s));
	}

}
