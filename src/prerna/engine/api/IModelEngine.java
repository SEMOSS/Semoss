package prerna.engine.api;

import java.util.Map;

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
	 * This initializes the model engine client based on the defined INIT SCRIPT. The process will remain alive for 15 minutes before
	 * automatically shutting down. The client itself will be used as the gateway for all users inferencing with the model engine
	 * @return
	*/
	void startServer();
	
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
	Map<String, String> ask(String question, String context, Insight insight, Map <String, Object> parameters);
	
	// TODO update embeddings to handle a List of strings and remove unnecessary params
	/**
	 * Passes a string question to the model client to be encoded as a vector
	 * 
	 * @param question		The string that needs to be encoded
	 * @param insight		The insight from where the call is being made. The insight holds user credentials, project information and conversation history tied to the insightId
	 * @param parameters	Additional parameters such as temperature, top_k, max_new_tokens etc
	 * @return	an encoded object based on how the LLM encodes strings
	 */
	Object embeddings(String question, Insight insight, Map <String, Object> parameters);

	Object model(String question, Insight insight, Map <String, Object> parameters);

}
