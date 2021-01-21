package prerna.util.gson;

import java.io.IOException;
import java.util.List;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;

public class OrQueryFilterAdapter extends AbstractSemossTypeAdapter<OrQueryFilter> implements IQueryFilterAdapterHelper {

	@Override
	public OrQueryFilter read(JsonReader in) throws IOException {
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
		OrQueryFilter value = readContent(in);
		in.endObject();
		return value;
	}
	
	@Override
	public OrQueryFilter readContent(JsonReader in) throws IOException {
		OrQueryFilter filters = new OrQueryFilter();
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
	public void write(JsonWriter out, OrQueryFilter value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		out.beginObject();
		out.name("type").value(IQueryFilter.QUERY_FILTER_TYPE.OR.toString());
		out.name("content");
		out.beginArray();
		List<IQueryFilter> filters = value.getFilterList();
		for(IQueryFilter f : filters) {
			TypeAdapter adapter = IQueryFilter.getAdapterForFilter(f.getQueryFilterType());
			adapter.write(out, f);
		}
		out.endArray();
		out.endObject();
	}

}
