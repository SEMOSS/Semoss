package prerna.reactor.playwright;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.playwright.Page;

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
                Path dir = Path.of("C:/workspace/Apps/recordings");
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
}