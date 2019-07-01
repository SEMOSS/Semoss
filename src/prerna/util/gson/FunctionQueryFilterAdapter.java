package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.filters.FunctionQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class FunctionQueryFilterAdapter extends TypeAdapter<FunctionQueryFilter> {
	
	@Override
	public FunctionQueryFilter read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		// might start with the type of the filter
		if(in.peek() == JsonToken.STRING) {
			in.nextString();
		}
		
		QueryFunctionSelectorAdapter adapter = new QueryFunctionSelectorAdapter();
		QueryFunctionSelector selector = null;
		in.beginObject();
		while(in.hasNext()) {
			// there is only 1 key
			in.nextName();
			selector = adapter.read(in);
		}
		in.endObject();
		
		FunctionQueryFilter filter = new FunctionQueryFilter();
		filter.setFunctionSelector(selector);
		return filter;
	}
	
	@Override
	public void write(JsonWriter out, FunctionQueryFilter value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		out.value(IQueryFilter.QUERY_FILTER_TYPE.FUNCTION.toString());
		
		QueryFunctionSelectorAdapter adapter = new QueryFunctionSelectorAdapter();
		
		out.beginObject();
		out.name("selector");
		adapter.write(out, value.getFunctionSelector());
		out.endObject();
	}
	
}
