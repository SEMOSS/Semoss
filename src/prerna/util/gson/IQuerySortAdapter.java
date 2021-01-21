package prerna.util.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySort;

public class IQuerySortAdapter extends AbstractSemossTypeAdapter<IQuerySort> {

	@Override
	public IQuerySort read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		
		// should start with the type
		in.beginObject();
		in.nextName();
		String sortTypeString = in.nextString();
		
		// get the correct adapter
		IQuerySort.QUERY_SORT_TYPE sortType = IQuerySort.convertStringToSortType(sortTypeString);
		IQuerySortAdapterHelper reader = (IQuerySortAdapterHelper) IQuerySort.getAdapterForSort(sortType);

		// now we should have the content object
		in.nextName();
		IQuerySort sort = reader.readContent(in);
		in.endObject();
		
		return sort;
	}
	
	@Override
	public void write(JsonWriter out, IQuerySort value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}

		// go to the specific instance impl to write it
		IQuerySort.QUERY_SORT_TYPE sortType = value.getQuerySortType();
		TypeAdapter reader = IQuerySort.getAdapterForSort(sortType);
		reader.write(out, value);
	}

}
