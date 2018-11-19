package prerna.util.gson;

import java.io.IOException;

import com.google.gson.stream.JsonReader;

import prerna.query.querystruct.selectors.IQuerySelector;

public interface IQueryTypeAdapter {

	IQuerySelector readContent(JsonReader in) throws IOException;
}
