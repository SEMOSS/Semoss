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

		String mapStr = in.nextString();
		Map<String, Object> map = GSON.fromJson(mapStr, Map.class);
		
		QueryFunctionSelector value = new QueryFunctionSelector();
		value.setFunction(map.get("function") + "");
		
		List<String> gsonInnerSelectors = (List<String>) map.get("innerSelectors");
		List<String> innerSelectorTypes = (List<String>) map.get("innerSelectorTypes");
		for(int iSelectorIndex = 0; iSelectorIndex < gsonInnerSelectors.size(); iSelectorIndex++) {
			String gsonInnerSelector = gsonInnerSelectors.get(iSelectorIndex);
			SELECTOR_TYPE type = IQuerySelector.convertStringToSelectorType(innerSelectorTypes.get(iSelectorIndex));
			value.addInnerSelector( (IQuerySelector) IQuerySelector.getGson().fromJson(gsonInnerSelector, IQuerySelector.getQuerySelectorClassFromType(type)) );
		}

		// optional setters
		if(map.containsKey("alias")) {
			value.setAlias(map.get("alias") + "");
		}
		if(map.containsKey("distinct")) {
			value.setDistinct( (boolean) map.get("distinct"));
		}
		if(map.containsKey("colCast")) {
			value.setColCast(map.get("colCast") + "");
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