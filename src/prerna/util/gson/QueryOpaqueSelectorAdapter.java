package prerna.util.gson;

import java.io.IOException;
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
		value.setTable(map.get("table"));
		return value;
	}

	@Override 
	public void write(JsonWriter out, QueryOpaqueSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		out.value(IQuerySelector.SELECTOR_TYPE.OPAQUE.toString());

		out.beginObject();
		out.name("alias").value(value.getAlias());
		out.name("table").value(value.getTable());
		out.name("querySyntax").value(value.getQuerySelectorSyntax());
		out.endObject();
	}
}