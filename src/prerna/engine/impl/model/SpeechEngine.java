package prerna.engine.impl.model;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.responses.SpeechModelEngineResponse;
import prerna.ds.py.PyUtils;
import prerna.om.Insight;
import prerna.util.UploadInputUtility;
import prerna.engine.api.ISpeechEngine;



public class SpeechEngine extends AbstractPythonModelEngine implements ISpeechEngine {
	
	private static final Logger classLogger = LogManager.getLogger(SpeechEngine.class);
	
	public SpeechModelEngineResponse generateSpeech(String prompt, Insight insight, Map<String, Object> parameters) {
		checkSocketStatus();
		
		String varName = getVarName();
		
		StringBuilder callMaker = new StringBuilder(varName + ".generate_speech(");
		
		callMaker.append("prompt").append("=").append(PyUtils.determineStringType(prompt));
		
		// Find the path to save the audio file
		String space = "";
		String filePath = "";
		if (parameters.containsKey("filePath")) {
			filePath = (String) parameters.get("filePath");
		}
		if (parameters.containsKey("space")) {
			space = (String) parameters.get("space");
		} else {
			space = "insight";
		}
		String outputDir = UploadInputUtility.getFilePath(insight, filePath, space);
		parameters.remove("filePath");
		parameters.remove("space");
		parameters.put("output_dir", outputDir);
		
		// Find the path for the voice file
		String speakerSpace = (String) parameters.get("speakerSpace");
		String speakerFilePath = (String) parameters.get("speakerFilePath");
		String speakerPath = UploadInputUtility.getFilePath(insight, speakerFilePath, speakerSpace);
		parameters.remove("speakerFilePath");
		parameters.remove("speakerSpace");
		parameters.put("speaker_file_path", speakerPath);
		
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
        
        SpeechModelEngineResponse response = SpeechModelEngineResponse.fromObject(output);
        
        return response;
	}
	
	public Map<String, Object> getSpectrogramModels(Insight insight){
		checkSocketStatus();
		
		String varName = getVarName();
		
		StringBuilder callMaker = new StringBuilder(varName + ".get_spectrogram_models()");
		
		classLogger.info("Running >>>" + callMaker.toString());

        Object output = pyt.runScript(callMaker.toString(), insight);
        
        List<String> models = (List<String>) ((Map<String, Object>) output).get("models");
        
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("models", models);
        
        return resultMap;
		
	}
	
	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.SPEECH;
	}
}
