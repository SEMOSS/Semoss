package prerna.query.querystruct.selectors.adapters;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;

public class QueryOpaqueSelectorAdapter extends TypeAdapter<QueryOpaqueSelector> {

	private static final Gson GSON = new Gson();
	
	@Override 
	public QueryOpaqueSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		String mapStr = in.nextString();
		Map<String, String> map = GSON.fromJson(mapStr, Map.class);

		QueryOpaqueSelector value = new QueryOpaqueSelector();
		value.setQuerySelectorSyntax(map.get("querySyntax"));
		value.setAlias(map.get("alias"));
		return value;
	}

	@Override 
	public void write(JsonWriter out, QueryOpaqueSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("selectorType", IQuerySelector.SELECTOR_TYPE.OPAQUE.toString());
		map.put("alias", value.getAlias());
		map.put("querySyntax", value.getQuerySelectorSyntax());

		out.value(GSON.toJson(map));
	}
}