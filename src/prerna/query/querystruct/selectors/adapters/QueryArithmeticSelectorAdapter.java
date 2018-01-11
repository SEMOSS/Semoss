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
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;

public class QueryArithmeticSelectorAdapter extends TypeAdapter<QueryArithmeticSelector> {
	
	private static final Gson GSON = new Gson();
	
	@Override
	public QueryArithmeticSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		String mapStr = in.nextString();
		Map<String, Object> map = GSON.fromJson(mapStr, Map.class);
		
		QueryArithmeticSelector value = new QueryArithmeticSelector();
		value.setMathExpr(map.get("mathExpr") + "");
		
		// get the left selector
		Map<String, Object> leftMap = (Map<String, Object>) map.get("lhs");
		String leftSelector = leftMap.get("selector").toString();
		SELECTOR_TYPE leftSelectorType = IQuerySelector.convertStringToSelectorType(leftMap.get("selectorType").toString());
		value.setLeftSelector( (IQuerySelector) IQuerySelector.getGson().fromJson(leftSelector, IQuerySelector.getQuerySelectorClassFromType(leftSelectorType)) );
		
		// now the right selector
		Map<String, Object> rightMap = (Map<String, Object>) map.get("rhs");
		String rightSelector = rightMap.get("selector").toString();
		SELECTOR_TYPE rightSelectorType = IQuerySelector.convertStringToSelectorType(rightMap.get("selectorType").toString());
		value.setRightSelector( (IQuerySelector) IQuerySelector.getGson().fromJson(rightSelector, IQuerySelector.getQuerySelectorClassFromType(rightSelectorType)) );
		
		// optional setters
		if(map.containsKey("alias")) {
			value.setAlias(map.get("alias") + "");
		}

		return value;
	}

	@Override
	public void write(JsonWriter out, QueryArithmeticSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("selectorType", IQuerySelector.SELECTOR_TYPE.ARITHMETIC.toString());
		map.put("alias", value.getAlias());
		map.put("mathExpr", value.getMathExpr());
		
		// need to handle left selectors
		Map<String, Object> leftMap = new HashMap<String, Object>();
		leftMap.put("selector", IQuerySelector.getGson().toJson(value.getLeftSelector()));
		leftMap.put("selectorType", value.getLeftSelector().getSelectorType().toString());
		map.put("lhs", leftMap);
		
		// now for right selector
		Map<String, Object> rightMap = new HashMap<String, Object>();
		rightMap.put("selector", IQuerySelector.getGson().toJson(value.getRightSelector()));
		rightMap.put("selectorType", value.getRightSelector().getSelectorType().toString());
		map.put("rhs", rightMap);

		out.value(GSON.toJson(map));
	}
}