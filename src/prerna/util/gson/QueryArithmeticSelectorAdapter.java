package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;

public class QueryArithmeticSelectorAdapter extends TypeAdapter<QueryArithmeticSelector> implements IQueryTypeAdapter {
	
	@Override
	public QueryArithmeticSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		// remove the beginning objects
		in.beginObject();
		in.nextName();
		in.nextString();
		in.nextName();
		QueryArithmeticSelector value = readContent(in);
		in.endObject();
		return value;
	}
	
	@Override
	public QueryArithmeticSelector readContent(JsonReader in) throws IOException {
		QueryArithmeticSelector value = new QueryArithmeticSelector();
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(key.equals("alias")) {
				value.setAlias(in.nextString());
			} else if(key.equals("mathExpr")) {
				value.setMathExpr(in.nextString());
			} else if(key.equals("left")) {
				in.beginArray();
				while(in.hasNext()) {
					if(in.peek() == JsonToken.STRING) {
						// this is the type of the left selector
						in.nextString();
					} else if(in.peek() == JsonToken.BEGIN_OBJECT) {
						IQuerySelectorAdapter leftAdapter = new IQuerySelectorAdapter();
						IQuerySelector leftSelector = leftAdapter.read(in);
						value.setLeftSelector(leftSelector);
					}
				}
				in.endArray();
			} else if(key.equals("right")) {
				in.beginArray();
				while(in.hasNext()) {
					if(in.peek() == JsonToken.STRING) {
						// this is the type of the left selector
						in.nextString();
					} else if(in.peek() == JsonToken.BEGIN_OBJECT) {
						IQuerySelectorAdapter rightAdapter = new IQuerySelectorAdapter();
						IQuerySelector rightSelector = rightAdapter.read(in);
						value.setRightSelector(rightSelector);
					}
				}
				in.endArray();
			}
		}
		in.endObject();
		return value;
	}

	@Override
	public void write(JsonWriter out, QueryArithmeticSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		// always start with the type of the query selector
		out.beginObject();
		out.name("type").value(IQuerySelector.SELECTOR_TYPE.ARITHMETIC.toString());
		out.name("content");
		
		// content object
		out.beginObject();
		out.name("alias").value(value.getAlias());
		out.name("mathExpr").value(value.getMathExpr());
		
		out.name("left");
		out.beginArray();
		IQuerySelector left = value.getLeftSelector();
		TypeAdapter leftOutput = IQuerySelector.getAdapterForSelector(left.getSelectorType());
		leftOutput.write(out, left);
		out.endArray();
		
		out.name("right");
		out.beginArray();
		IQuerySelector right = value.getRightSelector();
		TypeAdapter rightOutput = IQuerySelector.getAdapterForSelector(right.getSelectorType());
		rightOutput.write(out, right);
		out.endArray();
		out.endObject();
		
		out.endObject();
	}
}