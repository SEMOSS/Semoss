package prerna.util.gson;

import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SimpleQueryFilterAdapter extends TypeAdapter<SimpleQueryFilter> {
	
	private static final Gson GSON = GsonUtility.getDefaultGson();
	
	
	@Override
	public SimpleQueryFilter read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		// might start with the type of the filter
		if(in.peek() == JsonToken.STRING) {
			in.nextString();
		}
		
		NounMetadata left = null;
		NounMetadata right = null;
		String comparator = null;
		
		in.beginObject();
		while(in.hasNext()) {
			String name = in.nextName();
			if(name.equals("left")) {
				String lString = in.nextString();
				left = GSON.fromJson(lString, NounMetadata.class);
			} else if(name.equals("comparator")) {
				comparator =in.nextString();
			} else if(name.equals("right")) {
				String rightStr = in.nextString();
				right = GSON.fromJson(rightStr, NounMetadata.class);
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
		
		out.value(IQueryFilter.QUERY_FILTER_TYPE.SIMPLE.toString());
		
		NounMetadata left = value.getLComparison();
		NounMetadata right = value.getRComparison();
		String comp = value.getComparator();
		
		out.beginObject();
		out.name("left").value(GSON.toJson(left));
		out.name("comparator").value(comp);
		out.name("right").value(GSON.toJson(right));
		out.endObject();
	}

	
	
	
}
