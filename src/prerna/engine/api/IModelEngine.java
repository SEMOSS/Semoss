package prerna.engine.api;

import java.util.List;
import java.util.Map;

import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.ImageModelEngineResponse;
import prerna.om.Insight;

public interface IModelEngine extends IEngine {

	// this is what the FE sends for the type of storage we are creating
	// as a result, cannot be a key in the smss file
	String MODEL_TYPE = "MODEL_TYPE";
	
	// main class that is responsible for controlling everything models
	// hosting modes - embedded, inference_engine, FastChat, OpenAI
	// start server
	// connect client
	// disconnect client
	// stop server ?
	
	// reactors
	// ModelDeployMatchFinder - finds it based on GPU memory etc. 
	// StopModel
	
	/**
	 * Gets the type of the model inference engine.  The model engine type is often used to determine what client to use while running questions
	 * @return the type of the database 
	*/
	ModelTypeEnum getModelType();
		
	/**
	 * Passes the string question along with other parameters such as context and temperature to the python client and 
	 * 
	 * @param question 		The question being asked to the LLM
	 * @param context		(Optional) The context passed in by the user 
	 * @param insight		The insight from where the call is being made. The insight holds user credentials, project information and conversation history tied to the insightId
	 * @param parameters	Additional parameters such as temperature, top_k, max_new_tokens etc
	 * @return 	creates a map response with the following keys
	 * 				- response : The actual string response from the LLM/model
	 *  			- messageId : The unique identifier of a message (the user's input and the model response)
	 *  			- roomId: The insightId that the runPixel endpoint is being called from
	 */
	AskModelEngineResponse ask(String question, String context, Insight insight, Map <String, Object> parameters);
	

	/**
	 * Passes a list of strings to the model client to be embedded. Each string in the {@code stringsToEmbed} will be returned as its own vector.
	 * 
	 * @param stringsToEmbed	The string that needs to be encoded
	 * @param insight			The insight from where the call is being made. The insight holds user credentials, project information and conversation history tied to the insightId
	 * @param parameters		Additional parameters such as temperature, top_k, max_new_tokens etc
	 * @return					A list of embeddings
	 */
	EmbeddingsModelEngineResponse embeddings(List<String> stringsToEmbed, Insight insight, Map <String, Object> parameters);
	

	/**
	 * Passes a list of strings to the model client to be embedded. Each string in the {@code stringsToEmbed} will be returned as its own vector.
	 * 
	 * @param input				An input object to be sent to the ModelEngine. The input object should be serializable.
	 * @param insight			The insight from where the call is being made. The insight holds user credentials, project information and conversation history tied to the insightId
	 * @param parameters		Additional parameters
	 * @return					The output from the Model Engines model method
	 */
	Object model(Object input, Insight insight, Map <String, Object> parameters);
}
