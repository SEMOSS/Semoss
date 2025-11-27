package prerna.logging;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import prerna.engine.api.IEngine;

public class LoggingEngineSerializer implements JsonSerializer<IEngine> {

	@Override
	public JsonElement serialize(IEngine src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("engineId", src.getEngineId());
		jsonObject.addProperty("engineName", src.getEngineName());
		return jsonObject;
	}
}
