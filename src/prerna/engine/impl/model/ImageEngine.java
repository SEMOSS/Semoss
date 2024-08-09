package prerna.engine.impl.model;

import java.util.Iterator;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.impl.model.responses.ImageModelEngineResponse;
import prerna.engine.api.IImageEngine;

import prerna.engine.api.ModelTypeEnum;
import prerna.ds.py.PyUtils;
import prerna.om.Insight;
import prerna.util.UploadInputUtility;

public class ImageEngine extends AbstractPythonModelEngine implements IImageEngine {
	
	private static final Logger classLogger = LogManager.getLogger(ImageEngine.class);
	
	/**
	 * 	This method is responsible for building the Python script to generate an image.
	 * 
	 * @param prompt The description of the requested image.
	 * @param insight Current insight.
	 * @return A string representation of the response from the python script execution,
	 *         or an error message if the image generation fails.
	 */
	public ImageModelEngineResponse generateImage(String prompt, Insight insight, Map<String, Object> parameters) {
		checkSocketStatus();
		
		validateInputs(prompt, parameters);
		
		String varName = getVarName();

		StringBuilder callMaker = new StringBuilder(varName + ".generate_image(");
		
		callMaker.append("prompt").append("=").append(PyUtils.determineStringType(prompt));
		
		String outputDir = "";
		String space = "";
		String filePath = "";
		
		if (parameters != null) {
			if (parameters.containsKey("filePath")) {
				filePath = (String) parameters.get("filePath");
			}
			if (parameters.containsKey("space")) {
				space = (String) parameters.get("space");
			} else {
				space = "insight";
			}
		}
		
		outputDir = UploadInputUtility.getFilePath(insight, filePath, space);
		
		parameters.remove("filePath");
		parameters.remove("space");
		parameters.put("output_dir", outputDir);
		
		
		if(parameters != null) {
			Iterator <String> paramKeys = parameters.keySet().iterator();
			while(paramKeys.hasNext()) {
				String key = paramKeys.next();
				Object value = parameters.get(key);
				callMaker.append(",")
				         .append(key)
				         .append("=")
						 .append(PyUtils.determineStringType(value));
			}
		}
		
		callMaker.append(")");
		
		classLogger.info("Running >>>" + callMaker.toString());
		
        Object output = pyt.runScript(callMaker.toString(), insight);
		
		ImageModelEngineResponse response = ImageModelEngineResponse.fromObject(output);
        
        return response;
	}
	
	private void validateInputs(String prompt, Map<String, Object> parameters) {
		if (prompt == null || prompt.trim().isEmpty()) {
			throw new IllegalArgumentException("Prompt cannot be null or empty");
		}
		
		if (parameters != null) {
			if (parameters.containsKey("height")){
				validateNumericalParameter(parameters, "height");
			}
			if (parameters.containsKey("width")) {
				validateNumericalParameter(parameters, "width");
			}
			if (parameters.containsKey("guidance_scale")) {
				validateNumericalParameter(parameters, "guidance_scale");
			}
			if (parameters.containsKey("guidance_scale")) {
				validateNumericalParameter(parameters, "num_inference_step");
			}
			
		}
	}
	
	private void validateNumericalParameter(Map<String, Object> parameters, String paramName) {
		Object value = parameters.get(paramName);
		if (value != null) {
			Integer intValue = getInteger(value);
			if (intValue != null && intValue < 1) {
				throw new IllegalArgumentException(paramName + " cannot be less than 1");
			}
		}
	}
	
    protected static Integer getInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Double) {
            return ((Double) value).intValue();
        } else if (value instanceof String) {
            return Integer.valueOf((String) value);
        } else {
            return null;
        }
    }
	
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.IMAGE;
	}

}
 