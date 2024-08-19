package prerna.engine.impl.model.responses;

import java.util.Map;

public class SpeechModelEngineResponse extends AbstractSpeechModelEngineResponse<String> {
	
	private static final long serialVersionUID = 1;
	
	public SpeechModelEngineResponse(String file_path) {
		super(file_path);
	}
	
	public static SpeechModelEngineResponse fromMap(Map<String, Object> modelResponse) {
		String file_path = (String) modelResponse.get(FILE_PATH);
		
		return new SpeechModelEngineResponse(file_path);
	}
	
	@SuppressWarnings("unchecked")
	public static SpeechModelEngineResponse fromObject(Object responseObject) {
        Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
        return fromMap(modelResponse);
    }

}
