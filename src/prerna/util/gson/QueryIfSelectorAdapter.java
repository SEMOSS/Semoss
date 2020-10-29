package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryIfSelector;

public class QueryIfSelectorAdapter extends TypeAdapter<QueryIfSelector> implements IQuerySelectorAdapterHelper {

	@Override 
	public QueryIfSelector read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}

		// remove the beginning objects
		in.beginObject();
		in.nextName();
		in.nextString();
		in.nextName();
		
		// now we read the actual content
		QueryIfSelector value = readContent(in);
		in.endObject();
		return value;
	}

	@Override
	public QueryIfSelector readContent(JsonReader in) throws IOException {
		QueryIfSelector value = new QueryIfSelector();
		
		IQuerySelectorAdapter selectorAdapter = new IQuerySelectorAdapter();
		IQueryFilterAdapter filterAdapter = new IQueryFilterAdapter();
		
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(key.equals("precedent")) {
				IQuerySelector selector = selectorAdapter.read(in);
				value.setPrecedent(selector);
			} else if(key.equals("antecedent")) {
				IQuerySelector selector = selectorAdapter.read(in);
				value.setAntecedent(selector);
			} else if(key.equals("condition")) {
				IQueryFilter filter = filterAdapter.read(in);
				value.setCondition(filter);
			} else if(key.equals("alias")) {
				JsonToken peak = in.peek();
				if(peak == JsonToken.NULL) {
					in.nextNull();
				} else {
					value.setAlias(in.nextString());
				}
			} else if(key.equals("pixelString")) {
				// ignore
			}
		}
		in.endObject();
		return value;
	}

	@Override 
	public void write(JsonWriter out, QueryIfSelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		// always start with the type of the query selector
		out.beginObject();
		out.name("type").value(IQuerySelector.SELECTOR_TYPE.IF_ELSE.toString());
		out.name("content");
		// content object
		out.beginObject();
		out.name("precedent");
		{
			IQuerySelector querySelector = value.getPrecedent();
			TypeAdapter adapter = IQuerySelector.getAdapterForSelector(querySelector.getSelectorType());
			adapter.write(out, querySelector);
		}
		out.name("antecedent");
		{
			IQuerySelector querySelector = value.getAntecedent();
			TypeAdapter adapter = IQuerySelector.getAdapterForSelector(querySelector.getSelectorType());
			adapter.write(out, querySelector);
		}
		out.name("condition");
		{
			IQueryFilter condition = value.getCondition();
			TypeAdapter adapter = IQueryFilter.getAdapterForFilter(condition.getQueryFilterType());
			adapter.write(out, condition);
		}
		out.name("alias").value(value.getAlias());
		out.name("pixelString").value(value.getQueryStructName());
		out.endObject();
		out.endObject();
	}
	
}