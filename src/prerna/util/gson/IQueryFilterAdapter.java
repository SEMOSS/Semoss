package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.filters.IQueryFilter;

public class IQueryFilterAdapter extends TypeAdapter<IQueryFilter> {

	@Override
	public IQueryFilter read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		// should start with the type
		String filterTypeString = in.nextString();
		IQueryFilter.QUERY_FILTER_TYPE filterType = IQueryFilter.convertStringToFilterType(filterTypeString);
		
		TypeAdapter reader = IQueryFilter.getAdapterForFilter(filterType);
		return (IQueryFilter) reader.read(in);
	}

	@Override
	public void write(JsonWriter out, IQueryFilter value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		// go to the specific instance impl to write it
		IQueryFilter.QUERY_FILTER_TYPE filterType = value.getQueryFilterType();
		TypeAdapter reader = IQueryFilter.getAdapterForFilter(filterType);
		reader.write(out, value);
	}

}
