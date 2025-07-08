package prerna.engine.impl.model.message;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Type;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.impl.model.Room;
import prerna.om.Insight;

public class MessageUtils {

	private static final ExclusionStrategy NO_ROOM_INSIGHT_SOCKET_EXCLUSION = new ExclusionStrategy() {
	    @Override
	    public boolean shouldSkipField(FieldAttributes f) {
	        String fieldName = f.getName();
	        if ("room".equals(fieldName) || "insight".equals(fieldName))
	            return true;
	        Type declaredType = f.getDeclaredType();
	        if (declaredType instanceof Class<?>) {
	            Class<?> declaredClass = (Class<?>) declaredType;
	            if (Room.class.isAssignableFrom(declaredClass) ||
	                Insight.class.isAssignableFrom(declaredClass) ||
	                Socket.class.isAssignableFrom(declaredClass))
	                return true;
	        }
	        return false;
	    }
	    @Override
	    public boolean shouldSkipClass(Class<?> clazz) {
	        return Room.class.isAssignableFrom(clazz) ||
	               Insight.class.isAssignableFrom(clazz) ||
	               Socket.class.isAssignableFrom(clazz);
	    }
	};

	// For DB: skips "room", "insight", "socket", and "base64Data"
	private static final Gson gsonForDB = new GsonBuilder()
			.addSerializationExclusionStrategy(NO_ROOM_INSIGHT_SOCKET_EXCLUSION)
			.addSerializationExclusionStrategy(new ExclusionStrategy() {
				@Override
				public boolean shouldSkipField(FieldAttributes f) {
					return "base64Data".equals(f.getName());
				}

				@Override
				public boolean shouldSkipClass(Class<?> clazz) {
					return false;
				}
			}).create();

	// For Python: skips "room", "insight", "socket", includes base64Data
	private static final Gson gsonForPy = new GsonBuilder()
			.addSerializationExclusionStrategy(NO_ROOM_INSIGHT_SOCKET_EXCLUSION).create();

	private static Logger logger = LogManager.getLogger(MessageUtils.class);

	// ---- Serialization/Deserialization ----

	// Deserialize a single message from JSON
	public static AbstractMessage fromJson(String json) {
		JsonObject jsonObj = JsonParser.parseString(json).getAsJsonObject();
		MessageType type = MessageType.valueOf(jsonObj.get("type").getAsString());
		switch (type) {
		case RESPONSE_TEXT:
		case RESPONSE_TOOL:
			return gsonForDB.fromJson(json, ResponseMessage.class);
		case INPUT_TEXT:
		case INPUT_MEDIA:
		default:
			return gsonForDB.fromJson(json, InputMessage.class);
		}
	}

	// Serialize any message to JSON (for DB)
	public static String toJson(AbstractMessage msg) {
		return gsonForDB.toJson(msg);
	}

	// Deserialize from JSON array string to List<AbstractMessage>
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

	// --- Core two serialization methods ---

	// For DB: JSON array string of messages, with NO base64
	public static String toJsonArray(List<AbstractMessage> msgs) {
		if (msgs == null || msgs.isEmpty()) {
			return "[]";
		}
		return gsonForDB.toJson(msgs);
	}

	// For Python: JSON array string WITH base64 image data in ImageInfo
	public static String toJsonArrayWithImageData(List<AbstractMessage> msgs) {
		if (msgs == null || msgs.isEmpty()) {
			return "[]";
		}
		// Ensure base64Data is loaded for all images
		for (AbstractMessage msg : msgs) {
			if (msg instanceof InputMessage) {
				InputMessage input = (InputMessage) msg;
				if (input.hasImages()) {
					for (ImageInfo img : input.getImageInfos()) {
						// Populate the field (it will actually load the file if needed)
						img.setBase64Data(img.getBase64Data());
					}
				}
			}
		}
		return gsonForPy.toJson(msgs);
	}

	// ---- Utility/Convenience methods (maintain if needed) ----

	// These can alias to above or be retained for backwards compatibility
	public static String getMessagesForDatabase(List<AbstractMessage> msgs) {
		return toJsonArray(msgs);
	}

	public static String getMessagesForPy(List<AbstractMessage> msgs) {
		return toJsonArrayWithImageData(msgs);
	}

	// ---- Image copy utilities (unchanged) ----

	public static List<String> copyFilesToRoomFolder(List<String> relativePathToFiles, Room room, Insight insight) {
		List<String> roomFilePaths = new ArrayList<>();
		if (relativePathToFiles == null || relativePathToFiles.isEmpty()) {
			logger.info("No file paths provided to copy.");
			return roomFilePaths;
		}
		String insightFolder = insight.getInsightFolder(); // absolute path to insight folder
		String roomFolder = room.getRoomFolderPath(); // absolute path to room folder
		Path targetDir = Paths.get(roomFolder);
		try {
			Files.createDirectories(targetDir);
		} catch (IOException e) {
			logger.warn("Failed to create room folder: " + targetDir, e);
			return roomFilePaths;
		}
		for (String relPath : relativePathToFiles) {
			File srcFile = new File(insightFolder, relPath);
			if (!srcFile.exists() || !srcFile.isFile()) {
				logger.info("Source file file does not exist in insight folder: " + srcFile.getAbsolutePath());
				continue;
			}
			String fileName = srcFile.getName();
			Path destination = targetDir.resolve(fileName);
			try {
				Files.copy(srcFile.toPath(), destination, StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				logger.warn("Failed to copy file: " + srcFile.getAbsolutePath() + " to " + destination, e);
			}
			roomFilePaths.add(destination.toString());
		}
		return roomFilePaths;
	}
}