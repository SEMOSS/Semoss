package prerna.reactor.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ImageReactor extends AbstractReactor {

	// inputs from custom k8s_kserve_custom_predictors
	private String[] keysToGet;
	private int[] keyRequired;
	
	public ImageReactor() {
		this.keysToGet = new String[] { 
			ReactorKeysEnum.ENGINE.getKey(), 
			ReactorKeysEnum.COMMAND.getKey(),
			ReactorKeysEnum.IMAGE.getKey(), // do I have to take this in, I thought the LLM produces the image?
			ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
			// getting the parameters from 
			ReactorKeysEnum.NEGATIVE_PROMPT.getKey(),
			ReactorKeysEnum.HEIGHT.getKey(),
			ReactorKeysEnum.WIDTH.getKey(),
			ReactorKeysEnum.NUM_INFERENCE_STEPS.getKey(),
			ReactorKeysEnum.GUIDANCE_SCALE.getKey(),
			ReactorKeysEnum.SEED.getKey(),
			ReactorKeysEnum.NUM_IMAGES.getKey()
		};
		//  assuming the list of numbers is what is mandatory? 1 being mandatory, 0 being optional?
		this.keyRequired = new int[] { 
			1, // [0] index for Engine
			1, // [1] index for Prompt
			0, // [2] index for Image
			0, // [3] index for Param_Values_Map
			0, // [4] index for NegativePrompt
			0, // [5] index for Height
			0, // [6] index for Width
			0, // [7] index for InferenceStops
			0, // [8] index for GuidanceScale
			0, // [9] index for Seed
			0};// [10] index for NumImages 
	}
	// from the dict it goes {engine, prompt(or command), image, parameters}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException(
				"Engine is required for image generation."
			);
		}
		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}

		String prompt = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		if (prompt == null || prompt.isEmpty()) {
			throw new IllegalArgumentException(
				"Prompt is required for image generation."
			);
		}

		// im assuming this wont work since image isnt required anymore?
		String image = this.keyValue.get(this.keysToGet[2]); 

		// We do NOT want to decode base64 encoded images --- and this doesnt really matter anymore
		if (image.startsWith("http")) {
		    image = Utility.decodeURIComponent(image);
		} 

		Map<String, Object> paramMap = getMap();
		
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		} 

		paramMap.put("command", prompt);
		addIfPresentString(paramMap, 4, ReactorKeysEnum.NEGATIVE_PROMPT, "low quality, blurry");
		addIfPresentInt(paramMap, 5, ReactorKeysEnum.HEIGHT, 512);
		addIfPresentInt(paramMap, 6, ReactorKeysEnum.WIDTH, 512);
		addIfPresentInt(paramMap, 7, ReactorKeysEnum.NUM_INFERENCE_STEPS, 50);
		addIfPresentDouble(paramMap, 8, ReactorKeysEnum.GUIDANCE_SCALE, 7.5);
		addIfPresentInt(paramMap, 9, ReactorKeysEnum.SEED, 42);
		addIfPresentInt(paramMap, 10, ReactorKeysEnum.NUM_IMAGES, 1);

		IModelEngine modelEngine = Utility.getModel(engineId);

		/* taking this out for a second, im pretty sure this is what we are needing to return if Im not mistaken.
		The visionReactor changed the image and I think the ImageReactor produces 
		// i get lost what happens here till the return
		paramMap.put("image_url", image);
		*/
		
		Map<String, Object> output = modelEngine.ask(prompt, null, this.insight, paramMap).toMap();
		System.out.println("Raw Output: " + output);
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	// This is what the quick fixed added
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[3]);
        if(mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if(mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if(mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return null;
	}

	// stackoverflow
	private void addIfPresentString(Map<String, Object> paramMap, int keyIndex, ReactorKeysEnum key, String defaultString) {
		if (keyIndex >= 0 && keyIndex < this.keysToGet.length) { // making sure that the keyIndex is valid
			String value = this.keyValue.get(this.keysToGet[keyIndex]); // get value from the keyValue map
			if (value != null) { // does the value exist
				paramMap.put(key.getKey(), value); // if it does, add it to the map
			} else {
				paramMap.put(key.getKey(), defaultString);
			}
		}
	}

	private void addIfPresentInt(Map<String, Object> paramMap, int keyIndex, ReactorKeysEnum key, int defaultValue) {
		if (keyIndex >= 0 && keyIndex < this.keysToGet.length) { // same as above
			String value = this.keyValue.get(this.keysToGet[keyIndex]); // get the value
			if (value != null) { // if values exists turning to a integer
				paramMap.put(key.getKey(), Integer.parseInt(value));
			} else { // if it doesnt then input the default value
				paramMap.put(key.getKey(), defaultValue);
			}
		}
	}

	// same thinga as the one above but with a float for the guidance scale
	private void addIfPresentDouble(Map<String, Object> paramMap, int keyIndex, ReactorKeysEnum key, double defaultValue) {
		if (keyIndex >= 0 && keyIndex < this.keysToGet.length) { // same as above
			String value = this.keyValue.get(this.keysToGet[keyIndex]); // get the value
			if (value != null) { // if values exists turning to a float
				paramMap.put(key.getKey(), Double.parseDouble(value));
			} else { // if it doesnt then input the default value
				paramMap.put(key.getKey(), defaultValue);
			}
		}
	}

	/**
	 * 
	 * @return
	 */
	
	@Override
	public String getReactorDescription() {
		return "This method is used to run a image task with an Image-Text-to-Text model";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.COMMAND.getKey())) {
			return "This is the vision prompt to execute against the model";
		} else if(key.equals(ReactorKeysEnum.IMAGE.getKey())) {
			return "The image URL or base64 string for the vision task.";
		}
		return super.getDescriptionForKey(key);
	}

}
