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
import prerna.query.querystruct.selectors.QueryConstantSelector;

public class QueryConstantSelectorAdapter extends TypeAdapter<QueryConstantSelector> {

	private static final Gson GSON = new Gson();
	
	@Override 
	public QueryConstantSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		String mapStr = in.nextString();
		Map<String, Object> map = GSON.fromJson(mapStr, Map.class);

		QueryConstantSelector value = new QueryConstantSelector();
		value.setAlias(map.get("alias") + "");
		value.setConstant(map.get("constant"));
		return value;
	}

	@Override 
	public void write(JsonWriter out, QueryConstantSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("selectorType", IQuerySelector.SELECTOR_TYPE.CONSTANT.toString());
		map.put("alias", value.getAlias());
		map.put("constant", value.getConstant());
		
		out.value(GSON.toJson(map));
	}
}
