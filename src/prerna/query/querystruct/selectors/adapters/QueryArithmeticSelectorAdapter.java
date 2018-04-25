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

		QueryArithmeticSelector value = new QueryArithmeticSelector();

		Map<String, Object> LHS = new HashMap<String, Object>();
		Map<String, Object> RHS = new HashMap<String, Object>();

		in.beginObject();
		while(in.hasNext()) {
			if(in.peek() == JsonToken.STRING) {
				// this will be when we say this is a ARITHMATIC
			} else if(in.peek() == JsonToken.NAME){
				String key = in.nextName();
				if(key.equals("selectorType")) {
					// this will be when we say this is a ARITHMATIC
					in.nextString();
				} else if(key.equals("alias")) {
					value.setAlias(in.nextString());
				} else if(key.equals("mathExpr")) {
					value.setMathExpr(in.nextString());
				} else if(key.equals("lhs")) {
					in.beginObject();
					while(in.hasNext()) {
						String name = in.nextName();
						if(name.equals("selector")) {
							LHS.put("selector", in.nextString());
						} else if(name.equals("selectorType")) {
							LHS.put("selectorType", in.nextString());
						}
					}
					in.endObject();
				} else if(key.equals("rhs")) {
					in.beginObject();
					while(in.hasNext()) {
						String name = in.nextName();
						if(name.equals("selector")) {
							RHS.put("selector", in.nextString());
						} else if(name.equals("selectorType")) {
							RHS.put("selectorType", in.nextString());
						}
					}
					in.endObject();
				}
			}
		}
		in.endObject();
		
		// get the left selector
		String leftSelector = LHS.get("selector").toString();
		SELECTOR_TYPE leftSelectorType = IQuerySelector.convertStringToSelectorType(LHS.get("selectorType").toString());
		value.setLeftSelector( (IQuerySelector) IQuerySelector.getGson().fromJson(leftSelector, IQuerySelector.getQuerySelectorClassFromType(leftSelectorType)) );
		
		// now the right selector
		String rightSelector = RHS.get("selector").toString();
		SELECTOR_TYPE rightSelectorType = IQuerySelector.convertStringToSelectorType(RHS.get("selectorType").toString());
		value.setRightSelector( (IQuerySelector) IQuerySelector.getGson().fromJson(rightSelector, IQuerySelector.getQuerySelectorClassFromType(rightSelectorType)) );
		
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