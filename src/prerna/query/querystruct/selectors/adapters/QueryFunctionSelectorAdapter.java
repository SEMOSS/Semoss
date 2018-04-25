package prerna.query.querystruct.selectors.adapters;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class QueryFunctionSelectorAdapter extends TypeAdapter<QueryFunctionSelector> {
	
	private static final Gson GSON = new Gson();
	
	@Override
	public QueryFunctionSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		QueryFunctionSelector value = new QueryFunctionSelector();

		List<String> gsonInnerSelectors = new Vector<String>();
		List<String> innerSelectorTypes = new Vector<String>();
		
		in.beginObject();
		while(in.hasNext()) {
			if(in.peek() == JsonToken.STRING) {
				// this will be when we say this is a FUNCTION
			} else if(in.peek() == JsonToken.NAME){
				String key = in.nextName();
				if(key.equals("selectorType")) {
					// this will be when we say this is a FUNCTION
					in.nextString();
				} else if(key.equals("alias")) {
					value.setAlias(in.nextString());
				} else if(key.equals("distinct")) {
					value.setDistinct(in.nextBoolean());
				} else if(key.equals("colCast")) {
					value.setColCast(in.nextString());
				} else if(key.equals("function")) {
					value.setFunction(in.nextString());
				} else if(key.equals("innerSelectors")) {
					in.beginArray();
					while(in.hasNext()) {
						gsonInnerSelectors.add(in.nextString());
					}
					in.endArray();
				} else if(key.equals("innerSelectorTypes")) {
					in.beginArray();
					while(in.hasNext()) {
						innerSelectorTypes.add(in.nextString());
					}
					in.endArray();
				}
			}
		}
		in.endObject();
		
		for(int iSelectorIndex = 0; iSelectorIndex < gsonInnerSelectors.size(); iSelectorIndex++) {
			String gsonInnerSelector = gsonInnerSelectors.get(iSelectorIndex);
			SELECTOR_TYPE type = IQuerySelector.convertStringToSelectorType(innerSelectorTypes.get(iSelectorIndex));
			value.addInnerSelector( (IQuerySelector) IQuerySelector.getGson().fromJson(gsonInnerSelector, IQuerySelector.getQuerySelectorClassFromType(type)) );
		}

		return value;
	}

	@Override
	public void write(JsonWriter out, QueryFunctionSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("selectorType", IQuerySelector.SELECTOR_TYPE.FUNCTION.toString());
		map.put("alias", value.getAlias());
		map.put("function", value.getFunction());
		map.put("distinct", value.isDistinct());
		map.put("colCast", value.getColCast());
		
		// need to handle inner selectors
		List<IQuerySelector> innerSelectors = value.getInnerSelector();
		List<String> innerSelectorGson = new Vector<String>();
		List<String> innerSelectorTypes = new Vector<String>();
		for(IQuerySelector innerS : innerSelectors) {
			innerSelectorGson.add(IQuerySelector.getGson().toJson(innerS));
			innerSelectorTypes.add(innerS.getSelectorType().toString());
		}
		map.put("innerSelectors", innerSelectorGson);
		map.put("innerSelectorTypes", innerSelectorTypes);
		
		out.value(GSON.toJson(map));
	}
}