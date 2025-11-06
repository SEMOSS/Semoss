package prerna.logging;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import prerna.om.Insight;

public class LoggingInsightAdapter implements JsonSerializer<Insight> {

	@Override
	public JsonElement serialize(Insight src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("insightId", src.getInsightId());
		return jsonObject;
	}
}
