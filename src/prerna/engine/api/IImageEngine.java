package prerna.engine.api;

import java.util.List;
import java.util.Map;

import prerna.engine.impl.model.responses.ImageModelEngineResponse;
import prerna.om.Insight;

public interface IImageEngine extends IEngine {
	
	// this is what the FE sends for the type of storage we are creating
	// as a result, cannot be a key in the smss file
	String MODEL_TYPE = "MODEL_TYPE";
	
	/**
	 * Gets the type of the model inference engine.  The model engine type is often used to determine what client to use while running questions
	 * @return the type of the database 
	*/
	ModelTypeEnum getModelType();
	
	ImageModelEngineResponse generateImage(String prompt, Insight insight, Map<String, Object> parameters);

}
