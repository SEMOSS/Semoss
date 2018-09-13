package prerna.util.gson;

import java.io.IOException;
import java.util.List;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;

public class GenRowFiltersAdapter extends TypeAdapter<GenRowFilters> {

	@Override
	public GenRowFilters read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		GenRowFilters grf = new GenRowFilters();
		in.beginArray();
		while(in.hasNext()) {
			IQueryFilterAdapter filterAdapter = new IQueryFilterAdapter();
			IQueryFilter filter = filterAdapter.read(in);
			grf.addFilters(filter);
		}
		in.endArray();
		
		return grf;
	}

	@Override
	public void write(JsonWriter out, GenRowFilters value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		out.beginArray();
		List<IQueryFilter> filters = value.getFilters();
		int numFilters = filters.size();
		for(int i = 0; i < numFilters; i++) {
			IQueryFilter f = filters.get(i);
			TypeAdapter adapter = IQueryFilter.getAdapterForFilter(f.getQueryFilterType());
			adapter.write(out, f);
		}
		out.endArray();
	}

}
