package prerna.logging;

import java.lang.reflect.Type;
import java.sql.ResultSet;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class LoggingSQLResultSetSerializer implements JsonSerializer<ResultSet> {

	@Override
	public JsonElement serialize(ResultSet src, Type typeOfSrc, JsonSerializationContext context) {
		return new JsonPrimitive(System.identityHashCode(src));
	}
}
