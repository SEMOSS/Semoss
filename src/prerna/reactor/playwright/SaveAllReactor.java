package prerna.reactor.playwright;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class SaveAllReactor extends AbstractReactor {
	
    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	
	public SaveAllReactor(){
		this.keysToGet = new String[] {
				"sessionId",
				"name",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
				};
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
    	Map<String, Object> paramValues = Utility.getMap(this.store, this.curRow);

		String sessionId = this.keyValue.get(this.keysToGet[0]);
		String name = this.keyValue.get(this.keysToGet[1]);
		StepsEnvelope stepsEnvelope = json.convertValue(paramValues, StepsEnvelope.class);

		return new NounMetadata(saveAllToFile(sessionId,
				name, stepsEnvelope), PixelDataType.MAP);
	}
	
	public String saveAllToFile(String sessionId, String name, StepsEnvelope body) {
        // Build meta with timestamps
        long now = System.currentTimeMillis();

        // Try to preserve createdAt if file already exists
        String base = sanitize(name == null || name.isBlank() ? ("script-" + timestamp()) : name);
        Path file = ReplayFromFileReactor.recordingsDir.resolve(base.endsWith(".json") ? base : (base + ".json"));

        RecordingMeta existingMeta = null;
        if (Files.exists(file)) {
            try {
                StepsEnvelope existing = json.readValue(file.toFile(), StepsEnvelope.class);
                existingMeta = existing.meta();
            } catch (Exception ignored) {}
        }

        RecordingMeta newMeta = new RecordingMeta(
                (existingMeta != null && existingMeta.id() != null) ? existingMeta.id() : sessionId,
                body.meta().title(),
                body.meta().description(),
                (existingMeta != null && existingMeta.createdAt() != null) ? existingMeta.createdAt() : now,
                now
        );

        StepsEnvelope env = new StepsEnvelope("1.0", newMeta,body.steps() );

        try {
            json.writeValue(file.toFile(), env);
            // also replace in-memory session history so future actions reflect these edits
            Session s = SessionReactor.get(sessionId);
            s.history = env;
            return file.toAbsolutePath().toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to save script to: " + file, e);
        }
    }
	
    private String timestamp() {
        return DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").format(LocalDateTime.now());
    }
    
    private String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }


}
