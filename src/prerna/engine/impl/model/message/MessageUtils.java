package prerna.engine.impl.model.message;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

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
import prerna.cluster.util.ClusterUtil;

public class MessageUtils {
    // For DB: skips "room" and "base64Data"
    private static final Gson gsonForDB = new GsonBuilder()
        .addSerializationExclusionStrategy(new ExclusionStrategy() {
            @Override
            public boolean shouldSkipField(FieldAttributes field) {
                return "room".equals(field.getName()) || "base64Data".equals(field.getName());
            }
            @Override
            public boolean shouldSkipClass(Class<?> clazz) {
                return false;
            }
        })
        .setPrettyPrinting()
        .create();

    // For Python: skips "room" only, DOES include base64Data
    private static final Gson gsonForPy = new GsonBuilder()
        .addSerializationExclusionStrategy(new ExclusionStrategy() {
            @Override
            public boolean shouldSkipField(FieldAttributes field) {
                return "room".equals(field.getName());
            }
            @Override
            public boolean shouldSkipClass(Class<?> clazz) {
                return false;
            }
        })
        .setPrettyPrinting()
        .create();

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
                Files.copy(srcFile.toPath(), destination, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                logger.warn("Failed to copy file: " + srcFile.getAbsolutePath() + " to " + destination, e);
            }
        }
    }
}