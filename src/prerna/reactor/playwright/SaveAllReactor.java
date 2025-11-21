package prerna.reactor.playwright;

import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SaveAllReactor extends AbstractReactor {

	ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	public SaveAllReactor() {
		this.keysToGet = new String[] { "sessionId", "name", "title", "description" };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String sessionId = this.keyValue.get(this.keysToGet[0]);
		String name = this.keyValue.get(this.keysToGet[1]);
		String title = this.keyValue.get(this.keysToGet[2]);
		String desc = this.keyValue.get(this.keysToGet[3]);

		return new NounMetadata(saveAllToFile(sessionId, name, title, desc), PixelDataType.MAP);
	}

	public String saveAllToFile(String sessionId, String name, String title, String desc) {
		// Build meta with timestamps
		long now = System.currentTimeMillis();

		// Try to preserve createdAt if file already exists
		String base = PlaywrightUtility.sanitizeFilename(
				name == null || name.isBlank() ? ("script-" + PlaywrightUtility.generateTimestamp()) : name);
		Path file = PlaywrightUtility.initRecordingsDir().resolve(base.endsWith(".json") ? base : (base + ".json"));

		RecordingMeta existingMeta = null;
		if (Files.exists(file)) {
			try {
				StepsEnvelope existing = json.readValue(file.toFile(), StepsEnvelope.class);
				existingMeta = existing.meta();
			} catch (Exception ignored) {
			}
		}

		RecordingMeta newMeta = new RecordingMeta(
				(existingMeta != null && existingMeta.id() != null) ? existingMeta.id() : sessionId, title, desc,
				(existingMeta != null && existingMeta.createdAt() != null) ? existingMeta.createdAt() : now, now);

		PlaywrightSession s = this.insight.getUser().getPlaywrightSession(sessionId);

		StepsEnvelope env = new StepsEnvelope("1.0", newMeta, s.history.steps());

		try {
			json.writeValue(file.toFile(), env);
			return file.toAbsolutePath().toString();
		} catch (Exception e) {
			throw new RuntimeException("Failed to save script to: " + file, e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Reactor that replays step that is in order to run by given , sesionId, tabId, and fileName";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		} else if (key.equals("name")) {
			return "the name of the recorded file";
		} else if (key.equals("description")) {
			return "The description of the recorded file";
		} else if (key.equals("title")) {
			return "The title of the recorded file";
		}
		return super.getDescriptionForKey(key);
	}
}
