package prerna.reactor.playwright;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;

import org.json.JSONObject;
import org.json.JSONTokener;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reactor to retrieve the description of a Playwright recording file.
 * This reactor reads a recording JSON file and returns the description
 * from the metadata.
 */
public class GetRecordingDescriptionReactor extends AbstractReactor {

	private final static String SCRIPT_KEY = "Script";

	public GetRecordingDescriptionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), SCRIPT_KEY };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String fileName = this.keyValue.get(this.keysToGet[1]);

		if (fileName == null || fileName.trim().isEmpty()) {
			throw new IllegalArgumentException("File name cannot be null or empty");
		}

		if (!fileName.toLowerCase().endsWith(".json")) {
			fileName += ".json";
		}

		Path recordingsDir = PlaywrightUtility.initRecordingsDir(projectId);
		Path scriptPath = recordingsDir.resolve(fileName);

		File scriptFile = scriptPath.toFile();

		if (!scriptFile.exists()) {
			throw new IllegalArgumentException("Script file not found: " + fileName + " in recordings folder");
		}

		String description = "";

		try (FileReader reader = new FileReader(scriptFile)) {
			JSONTokener tokener = new JSONTokener(reader);
			JSONObject jsonObject = new JSONObject(tokener);

			if (jsonObject.has("meta")) {
				JSONObject meta = jsonObject.getJSONObject("meta");

				if (meta.has("description")) {
					description = meta.optString("description", "");
				}
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Error reading script file: " + fileName, e);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error parsing JSON from script file: " + fileName, e);
		}

		return new NounMetadata(description, PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return "Retrieves the description of a Playwright recording file from its metadata.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SCRIPT_KEY)) {
			return "The name of the JSON file (e.g., 'script-1.json') located in the recordings folder.";
		}

		return super.getDescriptionForKey(key);
	}
}

