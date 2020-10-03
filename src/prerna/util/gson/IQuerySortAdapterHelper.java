package prerna.util.gson;

import java.io.IOException;

import com.google.gson.stream.JsonReader;

import prerna.query.querystruct.selectors.IQuerySort;

public interface IQuerySortAdapterHelper {

	IQuerySort readContent(JsonReader in) throws IOException;
}
