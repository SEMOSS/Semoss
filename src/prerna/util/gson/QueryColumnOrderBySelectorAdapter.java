package prerna.util.gson;

import java.io.IOException;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;

public class QueryColumnOrderBySelectorAdapter extends AbstractSemossTypeAdapter<QueryColumnOrderBySelector> implements IQuerySortAdapterHelper {

	@Override 
	public QueryColumnOrderBySelector read(JsonReader in) throws IOException {
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
		QueryColumnOrderBySelector value = readContent(in);
		in.endObject();
		return value;
	}

	@Override
	public QueryColumnOrderBySelector readContent(JsonReader in) throws IOException {
		QueryColumnOrderBySelector value = new QueryColumnOrderBySelector();
		
		in.beginObject();
		while(in.hasNext()) {
			String key = in.nextName();
			if(key.equals("table")) {
				value.setTable(in.nextString());
			} else if(key.equals("column")) {
				value.setColumn(in.nextString());
			} else if(key.equals("alias")) {
				JsonToken peak = in.peek();
				if(peak == JsonToken.NULL) {
					in.nextNull();
				} else {
					value.setAlias(in.nextString());
				}
			} else if(key.equals("tableAlias")) {
				JsonToken peak = in.peek();
				if(peak == JsonToken.NULL) {
					in.nextNull();
				} else {
					value.setTableAlias(in.nextString());
				}
			} else if(key.equals("direction")) {
				value.setSortDir(in.nextString());
			}
		}
		in.endObject();
		return value;
	}

	@Override 
	public void write(JsonWriter out, QueryColumnOrderBySelector value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		
		// always start with the type of the query selector
		out.beginObject();
		out.name("type").value(IQuerySort.QUERY_SORT_TYPE.COLUMN.toString());
		out.name("content");
		// content object
		out.beginObject();
		out.name("table").value(value.getTable());
		out.name("column").value(value.getColumn());
		out.name("alias").value(value.getAlias());
		out.name("tableAlias").value(value.getTableAlias());
		out.name("direction").value(value.getSortDir().toString());
		out.endObject();
		out.endObject();
	}
}