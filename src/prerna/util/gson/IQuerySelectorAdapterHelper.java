package prerna.util.gson;

import java.io.IOException;

import com.google.gson.stream.JsonReader;

import prerna.query.querystruct.selectors.IQuerySelector;

public interface IQuerySelectorAdapterHelper {

	IQuerySelector readContent(JsonReader in) throws IOException;
}
