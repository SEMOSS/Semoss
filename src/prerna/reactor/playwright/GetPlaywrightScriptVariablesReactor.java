package prerna.reactor.playwright;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class GetPlaywrightScriptVariablesReactor extends AbstractReactor {
	
	private final static String REACTOR_DESCRIPTION = "Parse a Playwright script JSON file and extract all elements of type TYPE or VARIABLE, returning a map with label as key and text as value.";
	private final static String SCRIPT_KEY = "Script";
	private final static String SCRIPT_KEY_DESCRIPTION = "The name of the JSON file (e.g., 'script-1.json') located in the recordings folder.";
	
	public GetPlaywrightScriptVariablesReactor() {
		this.keysToGet = new String[] {SCRIPT_KEY};
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String fileName = this.keyValue.get(this.keysToGet[0]);
		
		if (fileName == null || fileName.trim().isEmpty()) {
			throw new IllegalArgumentException("File name cannot be null or empty");
		}
		
		if (!fileName.toLowerCase().endsWith(".json")) {
			fileName += ".json";
		}
		
		//  the full path to the recordings folder (same as PlaywrightReactor)
//		Path recordingsDir = Path.of(AssetUtility.getProjectAssetsFolder(this.insight.getContextProjectName(), this.insight.getContextProjectId()), "recordings");
    	Path recordingsDir = ReplayFromFileReactor.recordingsDir;
		Path scriptPath = recordingsDir.resolve(fileName);
		
		File scriptFile = scriptPath.toFile();
		
		if (!scriptFile.exists()) {
			throw new IllegalArgumentException("Script file not found: " + fileName + " in recordings folder");
		}
		
		Map<String, String> variables = new HashMap<>();
		
		try (FileReader reader = new FileReader(scriptFile)) {
			JSONTokener tokener = new JSONTokener(reader);
			JSONObject jsonObject = new JSONObject(tokener);
			
			if (jsonObject.has("meta")) {
		        JSONObject meta = jsonObject.getJSONObject("meta");

		        if (meta.has("title")) {
		            String title = meta.optString("title", "");
		            if (!title.isEmpty()) {
		                variables.put("title", title);
		            }
		        }

		        if (meta.has("description")) {
		            String desc = meta.optString("description", "");
		            if (!desc.isEmpty()) {
		                variables.put("description", desc);
		            }
		        }
		    }
			
			if (jsonObject.has("steps")) {
				JSONArray steps = jsonObject.getJSONArray("steps");
				
				for (int i = 0; i < steps.length(); i++) {
					JSONObject step = steps.getJSONObject(i);
					
					if (step.has("type")) {
						String type = step.getString("type");
						boolean isPassword = step.getBoolean("isPassword");
						
						// process TYPE or VARIABLE steps
						if ("TYPE".equals(type) || "VARIABLE".equals(type)) {
							// Extract label and text
							String label = step.optString("label", null);
							String text = step.optString("text", null);
							
							if (isPassword) {
								text = text + "~pass";
							}
							if (label != null && !label.trim().isEmpty() &&
								text != null && !text.trim().isEmpty()) {
								variables.put(label, text);
							}
						}
					}
				}
			}
			
		} catch (IOException e) {
			throw new IllegalArgumentException("Error reading script file: " + fileName, e);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error parsing JSON from script file: " + fileName, e);
		}
		
		return new NounMetadata(variables, PixelDataType.MAP);
	}
	
	@Override
	public String getReactorDescription() {
		return REACTOR_DESCRIPTION;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SCRIPT_KEY)) {
			return SCRIPT_KEY_DESCRIPTION;
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}