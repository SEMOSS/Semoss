package prerna.util.gson;

import java.io.IOException;

import com.google.gson.stream.JsonReader;

import prerna.query.querystruct.filters.IQueryFilter;

public interface IQueryFilterAdapterHelper {

	IQueryFilter readContent(JsonReader in) throws IOException;
}
