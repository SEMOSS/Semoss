package prerna.util.gson;

import java.io.IOException;

import com.google.gson.stream.JsonReader;

import prerna.query.querystruct.joins.IRelation;

public interface IRelationAdapterHelper {

	IRelation readContent(JsonReader in) throws IOException;
}
