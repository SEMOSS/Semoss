package prerna.engine.impl.model.message;

import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class MessagePartAdapter implements JsonSerializer<MessagePart>, JsonDeserializer<MessagePart> {

	@Override
	public JsonElement serialize(MessagePart src, Type typeOfSrc, JsonSerializationContext context) {
		if (src == null) {
			return null;
		}
		return context.serialize(src, src.getClass());
	}

	@Override
	public MessagePart deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		if (json == null || !json.isJsonObject()) {
			return new UnknownMessagePart();
		}

		JsonObject obj = json.getAsJsonObject();
		String rawType = obj.has("type") && !obj.get("type").isJsonNull() ? obj.get("type").getAsString() : null;
		MessagePartType partType = MessagePartType.UNKNOWN;
		if (rawType != null) {
			try {
				partType = MessagePartType.valueOf(rawType);
			} catch (IllegalArgumentException ignore) {
				partType = MessagePartType.UNKNOWN;
			}
		}

		switch (partType) {
		case TEXT:
			return context.deserialize(obj, TextMessagePart.class);
		case MEDIA:
			return context.deserialize(obj, MediaMessagePart.class);
		case TOOL_CALL:
			return context.deserialize(obj, ToolCallMessagePart.class);
		case TOOL_RESULT:
			return context.deserialize(obj, ToolResultMessagePart.class);
		case THINKING:
			return context.deserialize(obj, ThinkingMessagePart.class);
		case SYSTEM:
			return context.deserialize(obj, SystemMessagePart.class);
		default:
			Map<String, Object> data = context.deserialize(obj, LinkedHashMap.class);
			return new UnknownMessagePart(partType, data);
		}
	}
}
