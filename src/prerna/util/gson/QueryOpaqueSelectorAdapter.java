package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;

public class QueryOpaqueSelectorAdapter extends TypeAdapter<QueryOpaqueSelector> implements IQueryTypeAdapter {

	@Override 
	public QueryOpaqueSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		// remove the beginning objects
		in.beginObject();
		in.nextName();
		in.nextString();
		in.nextName();
		
		// now we read the actual context
		QueryOpaqueSelector value = readContent(in);
		in.endObject();
		return value;
	}

	@Override
	public QueryOpaqueSelector readContent(JsonReader in) throws IOException {
		QueryOpaqueSelector value = new QueryOpaqueSelector();
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(key.equals("table")) {
				value.setTable(in.nextString());
			} else if(key.equals("alias")) {
				value.setAlias(in.nextString());
			} else if(key.equals("querySyntax")) {
				value.setQuerySelectorSyntax(in.nextString());
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

		// always start with the type of the query selector
		out.beginObject();
		out.name("type").value(IQuerySelector.SELECTOR_TYPE.OPAQUE.toString());
		out.name("content");
		// content object
		out.beginObject();
		out.name("alias").value(value.getAlias());
		out.name("table").value(value.getTable());
		out.name("querySyntax").value(value.getQuerySelectorSyntax());
		out.endObject();
		out.endObject();
	}
}