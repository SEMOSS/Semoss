package prerna.util.gson;

import java.io.IOException;

import com.google.gson.stream.JsonReader;

import prerna.query.querystruct.AbstractQueryStruct;

public interface AbstractQueryStructAdapterHelper {

	AbstractQueryStruct readContent(JsonReader in) throws IOException;
}
