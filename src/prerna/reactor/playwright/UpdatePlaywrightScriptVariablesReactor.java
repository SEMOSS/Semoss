package prerna.reactor.playwright;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class UpdatePlaywrightScriptVariablesReactor extends AbstractReactor {
	
	private final static String REACTOR_DESCRIPTION = "Update TYPE and VARIABLE elements in a Playwright script JSON file with new values and save as a new file in the recordings folder.";
	private final static String SCRIPT_KEY = "Script";
	private final static String SCRIPT_KEY_DESCRIPTION = "The name of the original JSON file (e.g., 'script-1.json') located in the recordings folder.";
	private final static String VARIABLES_KEY = "Variables";
	private final static String VARIABLES_KEY_DESCRIPTION = "Map containing the label-value pairs to update in the script (e.g., {\"username\": \"newUser\", \"password\": \"newPass\"}).";
	private final static String OUTPUT_SCRIPT_KEY = "OutputScript";
	private final static String OUTPUT_SCRIPT_KEY_DESCRIPTION = "The name of the new JSON file to save (e.g., 'script-1-updated.json'). If not provided, will append '-updated' to the original filename.";
	
	public UpdatePlaywrightScriptVariablesReactor() {
		this.keysToGet = new String[] {SCRIPT_KEY, VARIABLES_KEY, OUTPUT_SCRIPT_KEY};
		this.keyRequired = new int[] { 1, 1, 0 }; 
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String fileName = this.keyValue.get(SCRIPT_KEY);
		String outputFileName = this.keyValue.get(OUTPUT_SCRIPT_KEY);

		
		if (fileName == null || fileName.trim().isEmpty()) {
			throw new IllegalArgumentException("Script file name cannot be null or empty");
		}
		
		if (!fileName.toLowerCase().endsWith(".json")) {
			fileName += ".json";
		}
		
		if (outputFileName == null || outputFileName.trim().isEmpty()) {
			String baseName = fileName.substring(0, fileName.lastIndexOf('.'));
			outputFileName = baseName + "-updated.json";
		} else if (!outputFileName.toLowerCase().endsWith(".json")) {
			outputFileName += ".json";
		}
		
		// Get the variables map from the noun store
		Map<String, String> variablesToUpdate = getVariablesMap();
		
		if (variablesToUpdate == null || variablesToUpdate.isEmpty()) {
			throw new IllegalArgumentException("Variables map cannot be null or empty");
		}
		
		//  the full path to the recordings folder (same as PlaywrightReactor)
//		Path recordingsDir = Path.of(AssetUtility.getProjectAssetsFolder(this.insight.getContextProjectName(), this.insight.getContextProjectId()), "recordings");
    	Path recordingsDir = ReplayFromFileReactor.recordingsDir;
		Path inputPath = recordingsDir.resolve(fileName);
		Path outputPath = recordingsDir.resolve(outputFileName);
		
		File inputFile = inputPath.toFile();
		
		if (!inputFile.exists()) {
			throw new IllegalArgumentException("Script file not found: " + fileName + " in recordings folder");
		}
		
		
		try (FileReader reader = new FileReader(inputFile)) {
			JSONTokener tokener = new JSONTokener(reader);
			JSONObject jsonObject = new JSONObject(tokener);
			
			if (jsonObject.has("steps")) {
				JSONArray steps = jsonObject.getJSONArray("steps");
				
				for (int i = 0; i < steps.length(); i++) {
					JSONObject step = steps.getJSONObject(i);
					
					if (step.has("type")) {
						String type = step.getString("type");

						// Only process TYPE or VARIABLE steps
						if ("TYPE".equals(type) || "VARIABLE".equals(type)) {
							String label = step.optString("label", null);

							
							if (label != null && !label.trim().isEmpty() && 
								variablesToUpdate.containsKey(label)) {
								
								String newValue = variablesToUpdate.get(label);
								step.put("text", newValue);
							}
						}
					}
				}
			}
			
			// Write the updated JSON to the output file
			try (FileWriter writer = new FileWriter(outputPath.toFile())) {
				writer.write(jsonObject.toString(2));
			}
			
		} catch (IOException e) {
			throw new IllegalArgumentException("Error reading/writing script file: " + e.getMessage(), e);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error parsing JSON from script file: " + fileName, e);
		}
		
		// Return the name of the new JSON file
		return new NounMetadata(outputFileName, PixelDataType.CONST_STRING);
	}
	
	@Override
	public String getReactorDescription() {
		return REACTOR_DESCRIPTION;
	}

	/**
	 * Get the variables map from the noun store
	 * @return Map of variable names to values
	 */
	private Map<String, String> getVariablesMap() {
		GenRowStruct mapGrs = this.store.getNoun(VARIABLES_KEY);
		Map<String, String> output = new HashMap<>();
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				for (int i = 0; i<mapInputs.size();i++) {
					output.putAll((Map<? extends String, ? extends String>) mapInputs.get(i).getValue());
				}
				return output;
			}
		}
		return null;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SCRIPT_KEY)) {
			return SCRIPT_KEY_DESCRIPTION;
		} else if (key.equals(VARIABLES_KEY)) {
			return VARIABLES_KEY_DESCRIPTION;
		} else if (key.equals(OUTPUT_SCRIPT_KEY)) {
			return OUTPUT_SCRIPT_KEY_DESCRIPTION;
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}