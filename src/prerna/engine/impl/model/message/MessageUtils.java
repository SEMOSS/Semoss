package prerna.engine.impl.model.message;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.om.Insight;

public class MessageUtils {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	// Deserialize a single message from JSON
	public static AbstractMessage fromJson(String json) {
		JsonObject jsonObj = JsonParser.parseString(json).getAsJsonObject();
		MessageType type = MessageType.valueOf(jsonObj.get("type").getAsString());

		switch (type) {
		case RESPONSE_TEXT:
		case RESPONSE_TOOL:
			return gson.fromJson(json, ResponseMessage.class);
		case INPUT_TEXT:
		case INPUT_MEDIA:
			// add tool calls, system, etc, as needed
		default:
			return gson.fromJson(json, InputMessage.class);
		}
	}

	// Serialize any message to JSON
	public static String toJson(AbstractMessage msg) {
		return gson.toJson(msg);
	}

	// Deserialize from JSON array string to List<UnifiedMessage>
	public static List<AbstractMessage> fromJsonArray(String jsonArrayString, Insight insight) {
		if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
			return new ArrayList<>();
		}

		JsonArray array = JsonParser.parseString(jsonArrayString).getAsJsonArray();
		List<AbstractMessage> result = new ArrayList<>();

		for (JsonElement elem : array) {
			AbstractMessage message = fromJson(elem.toString());
			if (message != null) {
				message.setInsight(insight);
				result.add(message);
			}
		}

		return result;
	}

	// Serialize List<UnifiedMessage> to JSON array string for DB storage
	public static String toJsonArray(List<AbstractMessage> msgs) {
		if (msgs == null || msgs.isEmpty()) {
			return "[]";
		}

//		// Create safe copies for serialization (strip sensitive data if needed)
//		List<AbstractMessage> safeCopies = new ArrayList<>();
//		for (AbstractMessage m : msgs) {
//			AbstractMessage safeCopy = createSafeCopyForSerialization(m);
//			safeCopies.add(safeCopy);
//		}

		return gson.toJson(msgs);
	}

	// Create a safe copy of the message for serialization (remove large binary
	// data, etc.)
//	private static AbstractMessage createSafeCopyForSerialization(AbstractMessage original) {
//		if (original instanceof InputMessage) {
//			InputMessage msg = (InputMessage) original;
//			if (msg.hasImages()) {
//				msg.setFormattedMessage(null);
//			}
//			return msg;
//		}
//
//		return original;
//	}

}