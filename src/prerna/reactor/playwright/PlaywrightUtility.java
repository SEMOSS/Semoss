package prerna.reactor.playwright;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.util.AssetUtility;

public class PlaywrightUtility {
    
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);
    
    private static Path recordingsDirectory = null;
    
    public static ObjectMapper getJsonMapper() {
        return JSON_MAPPER;
    }
    
    /**
     * Initialize and get the recordings directory
     * @param projectName Project name for context
     * @param projectId Project ID for context
     * @return Path to recordings directory
     */
    public static Path initRecordingsDir(String projectName, String projectId) {
        if (recordingsDirectory == null) {
            try {
                Path dir = Path.of(AssetUtility.getProjectAssetsFolder(projectName, projectId), "recordings");
                Files.createDirectories(dir);
                recordingsDirectory = dir;
            } catch (Exception ex) {
                throw new RuntimeException("Cannot create recordings directory", ex);
            }
        }
        return recordingsDirectory;
    }
    

    public static Path initRecordingsDir() {
        if (recordingsDirectory == null) {
            try {
                Path dir = Path.of("/Users/ntarek/Documents/SEMOSS/workspace/Semoss/Apps/Recordings");
                Files.createDirectories(dir);
                recordingsDirectory = dir;
            } catch (Exception ex) {
                throw new RuntimeException("Cannot create recordings directory", ex);
            }
        }
        return recordingsDirectory;
    }
    
    /**
     * Load steps from a JSON file
     * @param nameOrPath File name or full path
     * @return StepsEnvelope containing the loaded steps
     */
    public static StepsEnvelope loadStepsFromFile(String nameOrPath) {
        Path file = resolveRecordingPath(nameOrPath);
        try {
            return JSON_MAPPER.readValue(file.toFile(), StepsEnvelope.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read: " + file, e);
        }
    }
    
    /**
     * Resolve a recording file path from name or full path
     * @param nameOrPath File name or full path
     * @return Resolved Path
     */
    public static Path resolveRecordingPath(String nameOrPath) {
        if (nameOrPath.contains(java.nio.file.FileSystems.getDefault().getSeparator())) {
            return Paths.get(nameOrPath);
        } else {
            String fileName = nameOrPath.endsWith(".json") ? nameOrPath : nameOrPath + ".json";
            return initRecordingsDir().resolve(fileName);
        }
    }
    
    /**
     * Generate timestamp string for file naming
     * @return Formatted timestamp string
     */
    public static String generateTimestamp() {
        return DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(LocalDateTime.now());
    }
    
    /**
     * Sanitize filename by replacing invalid characters
     * @param name Original filename
     * @return Sanitized filename
     */
    public static String sanitizeFilename(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    public static String callModel(String insightFolder, String imageName, ScreenshotResponse croppedImage,
                                   IModelEngine modelEngine, String instruction, Insight insight)
            throws IOException {
        String tempImagePath = Paths.get(insightFolder).resolve(imageName).toString();

        byte[] imageBytes = Base64.getDecoder().decode(croppedImage.base64Png());
        try (FileOutputStream fos = new FileOutputStream(tempImagePath)) {
            fos.write(imageBytes);
        }

        Room room = RoomUtils.createRoomIfNotExists(
                UUID.randomUUID().toString(),
                insight,
                modelEngine, null, null, null, null
        );

        List<String> copiedImages = MessageUtils.copyFilesToRoomFolder(
                Arrays.asList(imageName),
                room,
                insight
        );

        InputMessage inputMessage = InputMessage.builder(room)
                .withInputUIPrompt(instruction)
                .withInputPrompt(instruction)
                .withImage(copiedImages.getFirst(), room)
                .build();

        ResponseMessage response = room.ask(inputMessage, modelEngine);
        return response.getContent();
    }
}