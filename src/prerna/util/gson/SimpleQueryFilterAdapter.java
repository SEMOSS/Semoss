package prerna.util.gson;

import java.io.IOException;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SimpleQueryFilterAdapter extends AbstractSemossTypeAdapter<SimpleQueryFilter> implements IQueryFilterAdapterHelper {
	
	@Override
	public SimpleQueryFilter read(JsonReader in) throws IOException {
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
		SimpleQueryFilter value = readContent(in);
		in.endObject();
		return value;
	}
	
	@Override
	public SimpleQueryFilter readContent(JsonReader in) throws IOException {
		NounMetadataAdapter adapter = new NounMetadataAdapter();

		NounMetadata left = null;
		NounMetadata right = null;
		String comparator = null;
		
		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(name.equals("left")) {
				left = adapter.read(in);
			} else if(name.equals("comparator")) {
				comparator = in.nextString();
			} else if(name.equals("right")) {
				right = adapter.read(in); 
			}
		}
		in.endObject();
		
		return new SimpleQueryFilter(left, comparator, right);
	}
	
	@Override
	public void write(JsonWriter out, SimpleQueryFilter value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		NounMetadata left = value.getLComparison();
		NounMetadata right = value.getRComparison();
		String comp = value.getComparator();
		
		NounMetadataAdapter adapter = new NounMetadataAdapter();
		
		out.beginObject();
		out.name("type").value(IQueryFilter.QUERY_FILTER_TYPE.SIMPLE.toString());
		out.name("content");
			// content object
			out.beginObject();
				out.name("left");
				adapter.write(out, left);
				out.name("comparator").value(comp);
				out.name("right");
				adapter.write(out, right);
			out.endObject();
		out.endObject();
	}

}
