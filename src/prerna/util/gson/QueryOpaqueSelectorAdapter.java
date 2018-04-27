package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;

public class QueryOpaqueSelectorAdapter extends TypeAdapter<QueryOpaqueSelector> {

	@Override 
	public QueryOpaqueSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		QueryOpaqueSelector value = new QueryOpaqueSelector();
		in.beginObject();
		while(in.hasNext()) {
			if(in.peek() == JsonToken.NAME) {
				String key = in.nextName();
				if(key.equals("table")) {
					value.setTable(in.nextString());
				} else if(key.equals("alias")) {
					value.setAlias(in.nextString());
				} else if(key.equals("querySyntax")) {
					value.setQuerySelectorSyntax(in.nextString());
				}
			}
		}
		in.endObject();

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