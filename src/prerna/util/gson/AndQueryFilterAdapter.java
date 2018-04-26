package prerna.util.gson;

import java.io.IOException;
import java.util.List;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;

public class AndQueryFilterAdapter extends TypeAdapter<AndQueryFilter> {

	@Override
	public AndQueryFilter read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		// might start with the type of the filter
		if(in.peek() == JsonToken.STRING) {
			in.nextString();
		}
		
		AndQueryFilter filters = new AndQueryFilter();
		
		in.beginArray();
		while(in.hasNext()) {
			IQueryFilterAdapter adapter = new IQueryFilterAdapter();
			IQueryFilter f = adapter.read(in);
			filters.addFilter(f);
		}
		in.endArray();
		
		return filters;
	}
	
	@Override
	public void write(JsonWriter out, AndQueryFilter value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		out.value(IQueryFilter.QUERY_FILTER_TYPE.AND.toString());
				
		out.beginArray();
		List<IQueryFilter> filters = value.getFilterList();
		for(IQueryFilter f : filters) {
			TypeAdapter adapter = IQueryFilter.getAdapterForFilter(f.getQueryFilterType());
			adapter.write(out, f);
		}
		out.endArray();
	}


}
