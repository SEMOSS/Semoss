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
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class QueryColumnSelectorAdapter extends TypeAdapter<QueryColumnSelector> {

	private static final Gson GSON = new Gson();

	@Override 
	public QueryColumnSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		String mapStr = in.nextString();

		Map<String, String> map = GSON.fromJson(mapStr, Map.class);
		QueryColumnSelector value = new QueryColumnSelector();
		
		value.setTable(map.get("table"));
		value.setTableAlias(map.get("tableAlias"));
		value.setColumn(map.get("column"));
		value.setAlias(map.get("alias"));
		return value;
	}

	@Override 
	public void write(JsonWriter out, QueryColumnSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		Map<String, String> map = new HashMap<String, String>();
		map.put("selectorType", IQuerySelector.SELECTOR_TYPE.COLUMN.toString());
		map.put("table", value.getTable());
		map.put("tableAlias", value.getTableAlias());
		map.put("column", value.getColumn());
		map.put("alias", value.getAlias());
		
		out.value(GSON.toJson(map));
	}
}