package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;

public class QueryConstantSelectorAdapter extends TypeAdapter<QueryConstantSelector> {

	@Override 
	public QueryConstantSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		// might start with the type of the query selector
		if(in.peek() == JsonToken.STRING) {
			in.nextString();
		}
		
		QueryConstantSelector value = new QueryConstantSelector();
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(key.equals("selectorType")) {
				in.nextString();
			} else if(key.equals("alias")) {
				value.setAlias(in.nextString());
			} else if(key.equals("constant")) {
				if(in.peek() == JsonToken.NUMBER) {
					value.setConstant(in.nextDouble());
				} else {
					value.setConstant(in.nextString());
				}
			}
		}
		return value;
	}

	@Override 
	public void write(JsonWriter out, QueryConstantSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		// always start with the type of the query selector
		out.value(IQuerySelector.SELECTOR_TYPE.CONSTANT.toString());
		
		out.beginObject();
		out.name("alias").value(value.getAlias());
		Object val = value.getConstant();
		if(val instanceof Number) {
			out.name("constant").value((Number) val);
		} else {
			out.name("constant").value(val.toString());
		}
		out.endObject();
	}
}
