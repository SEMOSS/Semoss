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
		
		QueryColumnSelector value = new QueryColumnSelector();
		
		/*
		 * Sometimes, this comes in as an object, and other times as a string...
		 * Need to come back and figure out why
		 */
		if(in.peek() == JsonToken.BEGIN_OBJECT) {
			in.beginObject();
			while(in.hasNext()) {
				if(in.peek() == JsonToken.STRING) {
					// this will be when we say this is a COLUMN
				} else if(in.peek() == JsonToken.NAME) {
					String key = in.nextName();
					if(key.equals("selectorType")) {
						// this will be when we say this is a COLUMN
					} else if(key.equals("table")) {
						value.setTable(in.nextString());
					} else if(key.equals("tableAlias")) {
						value.setTableAlias(in.nextString());
					} else if(key.equals("column")) {
						value.setColumn(in.nextString());
					} else if(key.equals("alias")) {
						value.setAlias(in.nextString());
					}
				}
			}
			in.endObject();
		} else {
			String mapStr = in.nextString();

			Map<String, String> map = GSON.fromJson(mapStr, Map.class);
			value.setTable(map.get("table"));
			value.setTableAlias(map.get("tableAlias"));
			value.setColumn(map.get("column"));
			value.setAlias(map.get("alias"));
		}
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