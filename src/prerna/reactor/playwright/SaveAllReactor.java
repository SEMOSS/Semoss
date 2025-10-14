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
				"title",
				"description"
				};
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String sessionId = this.keyValue.get(this.keysToGet[0]);
		String name = this.keyValue.get(this.keysToGet[1]);
		String title = this.keyValue.get(this.keysToGet[2]);
		String desc = this.keyValue.get(this.keysToGet[3]);

		return new NounMetadata(saveAllToFile(sessionId,name, title, desc), PixelDataType.MAP);
	}
	
	public String saveAllToFile(String sessionId, String name, String title, String desc) {
        // Build meta with timestamps
        long now = System.currentTimeMillis();

        // Try to preserve createdAt if file already exists
        String base = PlaywrightUtility.sanitizeFilename(name == null || name.isBlank() ? ("script-" + PlaywrightUtility.generateTimestamp()) : name);
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
                title,
                desc,
                (existingMeta != null && existingMeta.createdAt() != null) ? existingMeta.createdAt() : now,
                now
        );
        
        Session s = SessionReactor.get(sessionId);
		StepsEnvelope env = new StepsEnvelope("1.0", newMeta,s.history.steps());

        try {
            json.writeValue(file.toFile(), env);
            return file.toAbsolutePath().toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to save script to: " + file, e);
        }
    }
}
