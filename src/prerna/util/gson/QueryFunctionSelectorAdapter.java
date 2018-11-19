package prerna.util.gson;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class QueryFunctionSelectorAdapter extends TypeAdapter<QueryFunctionSelector> implements IQueryTypeAdapter {
	
	@Override
	public QueryFunctionSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		// might start with the type of the query selector
		if(in.peek() == JsonToken.STRING) {
			in.nextString();
		}

		// remove the beginning objects
		in.beginObject();
		in.nextName();
		in.nextString();
		in.nextName();
		QueryFunctionSelector value = readContent(in);
		in.endObject();
		return value;
	}
	
	@Override
	public QueryFunctionSelector readContent(JsonReader in) throws IOException {
		QueryFunctionSelector value = new QueryFunctionSelector();
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(key.equals("alias")) {
				value.setAlias(in.nextString());
			} else if(key.equals("distinct")) {
				value.setDistinct(in.nextBoolean());
			} else if(key.equals("colCast")) {
				value.setColCast(in.nextString());
			} else if(key.equals("function")) {
				value.setFunction(in.nextString());
			} else if(key.equals("innerSelectors")) {
				List<IQuerySelector> innerList = new Vector<IQuerySelector>();
				
				in.beginArray();
				while(in.hasNext()) {
					IQuerySelectorAdapter innerAdapter = new IQuerySelectorAdapter();
					IQuerySelector innerSelector = innerAdapter.read(in);
					innerList.add(innerSelector);
				}
				in.endArray();
				
				value.setInnerSelector(innerList);
			}
		}
		in.endObject();
		return value;
	}


	@Override
	public void write(JsonWriter out, QueryFunctionSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		// always start with the type of the query selector
		out.beginObject();
		out.name("type").value(IQuerySelector.SELECTOR_TYPE.FUNCTION.toString());
		out.name("content");

		// content object
		out.beginObject();
		out.name("alias").value(value.getAlias());
		out.name("function").value(value.getFunction());
		out.name("distinct").value(value.isDistinct());
		out.name("colCast").value(value.getColCast());

		out.name("innerSelectors");
		out.beginArray();
		List<IQuerySelector> innerList =  value.getInnerSelector();
		for(IQuerySelector inner : innerList) {
			TypeAdapter leftOutput = IQuerySelector.getAdapterForSelector(inner.getSelectorType());
			leftOutput.write(out, inner);
		}
		out.endArray();
		out.endObject();
		
		out.endObject();
	}
}