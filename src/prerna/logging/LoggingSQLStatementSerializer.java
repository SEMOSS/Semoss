package prerna.logging;

import java.lang.reflect.Type;
import java.sql.Statement;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class LoggingSQLStatementSerializer implements JsonSerializer<Statement> {

	@Override
	public JsonElement serialize(Statement src, Type typeOfSrc, JsonSerializationContext context) {
		return new JsonPrimitive(System.identityHashCode(src));
	}
}
