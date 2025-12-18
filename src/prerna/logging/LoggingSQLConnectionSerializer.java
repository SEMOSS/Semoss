package prerna.logging;

import java.lang.reflect.Type;
import java.sql.Connection;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class LoggingSQLConnectionSerializer implements JsonSerializer<Connection> {

	@Override
	public JsonElement serialize(Connection src, Type typeOfSrc, JsonSerializationContext context) {
		return new JsonPrimitive(System.identityHashCode(src));
	}
}
