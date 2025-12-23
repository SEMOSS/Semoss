package prerna.logging;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import prerna.reactor.IReactor;

public class LoggingIReactorSerializer implements JsonSerializer<IReactor> {

	@Override
	public JsonElement serialize(IReactor src, Type typeOfSrc, JsonSerializationContext context) {
		return new JsonPrimitive(System.identityHashCode(src));
	}
}
