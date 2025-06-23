package prerna.engine.impl.model.message;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Modifier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


import prerna.engine.impl.model.Room;
import prerna.om.Insight;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;

public class MessageUtils {

	private static final Gson gson = new GsonBuilder()
		    .addSerializationExclusionStrategy(new ExclusionStrategy() {
		        @Override
		        public boolean shouldSkipField(FieldAttributes field) {
		            return field.getName().equals("room");
		        }

		        @Override
		        public boolean shouldSkipClass(Class<?> clazz) {
		            return false;
		        }
		    })
		    .setPrettyPrinting()
		    .create();
	private static Logger logger = LogManager.getLogger(MessageUtils.class);

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
	public static List<AbstractMessage> fromJsonArray(String jsonArrayString, Room room) {
		if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
			return new ArrayList<>();
		}

		JsonArray array = JsonParser.parseString(jsonArrayString).getAsJsonArray();
		List<AbstractMessage> result = new ArrayList<>();

		for (JsonElement elem : array) {
			AbstractMessage message = fromJson(elem.toString());
			if (message != null) {
				message.setRoom(room);
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

	public static void copyImagesToRoomFolder(List<String> relativePathToImage, Room room, Insight insight) {
	    if (relativePathToImage == null || relativePathToImage.isEmpty()) {
	        logger.info("No image paths provided to copy.");
	        return;
	    }

	    String insightFolder = insight.getInsightFolder(); // absolute path to insight folder
	    String roomFolder = room.getRoomFolderPath(); // absolute path to room folder

	    Path targetDir = Paths.get(roomFolder);

	    try {
	        Files.createDirectories(targetDir);
	    } catch (IOException e) {
	        logger.warn("Failed to create room folder: " + targetDir, e);
	        return;
	    }

	    for (String relPath : relativePathToImage) {
	        File srcFile = new File(insightFolder, relPath);
	        if (!srcFile.exists() || !srcFile.isFile()) {
	            logger.info("Source image file does not exist in insight folder: " + srcFile.getAbsolutePath());
	            continue;
	        }

	        String fileName = srcFile.getName();
	        Path destination = targetDir.resolve(fileName);

	        try {
	            Files.copy(
	                srcFile.toPath(),
	                destination,
	                StandardCopyOption.REPLACE_EXISTING
	            );
	        } catch (IOException e) {
	            logger.warn("Failed to copy file: " + srcFile.getAbsolutePath() + " to " + destination, e);
	        }
	    }
	}
}