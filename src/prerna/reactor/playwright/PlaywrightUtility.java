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

    public static String callModel(String insightFolder, String imageName, ScreenshotResponse croppedImage,
                                   IModelEngine modelEngine, String instruction, Insight insight) throws IOException {
        return callModel(insightFolder, imageName, croppedImage, modelEngine, instruction, insight, UUID.randomUUID().toString());
    }
    public static String callModel(String insightFolder, String imageName, ScreenshotResponse croppedImage,
                                   IModelEngine modelEngine, String instruction, Insight insight, String roomId)
            throws IOException {
        String tempImagePath = Paths.get(insightFolder).resolve(imageName).toString();

        byte[] imageBytes = Base64.getDecoder().decode(croppedImage.base64Png());
        try (FileOutputStream fos = new FileOutputStream(tempImagePath)) {
            fos.write(imageBytes);
        }

        Room room = RoomUtils.createRoomIfNotExists(
                roomId,
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

    public static boolean matchesSelector(Selector stepSelector, ElementProbeResponse probe) {
        if (stepSelector == null || probe == null) {
            return false;
        }

        String stepStrategy = stepSelector.strategy();
        String stepValue = stepSelector.value();

        switch (stepStrategy) {
            case "testId":
                // check data-testid or data-test-id attributes
                if (probe.attrs() == null) return false;
                String testId = probe.attrs().get("data-testid");
                if (testId == null) testId = probe.attrs().get("data-test-id");
                return stepValue.equals(testId);

            case "role":
                return stepValue.equals(probe.role());

            case "id":
                // check if probes id attribute matches
                if (probe.attrs() == null) return false;
                String id = probe.attrs().get("id");
                return id != null && !id.contains("::") && stepValue.equals(id);

            case "css":
                // css selector match
                if (probe.selector() == null) return false;

                // exact match
                if (stepValue.equals(probe.selector())) {
                    return true;
                }

                // If step selector is "body"
                if ("body".equals(stepValue)) {
                    return true;
                }

                // the probe's CSS path should contain or be contained by the step's CSS
                return probe.selector().contains(stepValue) || stepValue.contains(probe.selector());

            case "text":
                return probe.labelText() != null && probe.labelText().contains(stepValue);

            case "placeholder":
                return stepValue.equals(probe.placeholder());

            case "xpath":
                return matchesXpathAttributes(stepValue, probe);

            default:
                return false;
        }
    }

    private static boolean matchesXpathAttributes(String xpath, ElementProbeResponse probe) {
        if (probe.attrs() == null) {
            return false;
        }

        // Extract attribute conditions from XPath like [@id='value'] or [@class='value']
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\[@([^=]+)=['\"]([^'\"]+)['\"]\\]");
        java.util.regex.Matcher matcher = pattern.matcher(xpath);

        boolean foundAttribute = false;
        while (matcher.find()) {
            foundAttribute = true;
            String attrName = matcher.group(1).trim();
            String attrValue = matcher.group(2).trim();

            String actualValue = probe.attrs().get(attrName);
            if (actualValue == null || !actualValue.equals(attrValue)) {
                return false;
            }
        }
        return foundAttribute;
    }
}