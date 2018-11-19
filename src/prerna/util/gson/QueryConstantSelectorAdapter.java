package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;

public class QueryConstantSelectorAdapter extends TypeAdapter<QueryConstantSelector> implements IQueryTypeAdapter {

	@Override 
	public QueryConstantSelector read(JsonReader in) throws IOException {
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
		QueryConstantSelector value = readContent(in);
		in.endObject();
		return value;
	}

	@Override
	public QueryConstantSelector readContent(JsonReader in) throws IOException {
		QueryConstantSelector value = new QueryConstantSelector();
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(key.equals("alias")) {
				value.setAlias(in.nextString());
			} else if(key.equals("constant")) {
				if(in.peek() == JsonToken.NUMBER) {
					value.setConstant(in.nextDouble());
				} else {
					value.setConstant(in.nextString());
				}
			}
		}
		in.endObject();
		return value;
	}
	
	@Override 
	public void write(JsonWriter out, QueryConstantSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		// always start with the type of the query selector
		out.beginObject();
		out.name("type").value(IQuerySelector.SELECTOR_TYPE.CONSTANT.toString());
		out.name("content");
		// content object
		out.beginObject();
		out.name("alias").value(value.getAlias());
		Object val = value.getConstant();
		if(val instanceof Number) {
			out.name("constant").value((Number) val);
		} else {
			out.name("constant").value(val.toString());
		}
		out.endObject();
		out.endObject();
	}
}
