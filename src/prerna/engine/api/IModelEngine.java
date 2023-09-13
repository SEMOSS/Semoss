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
	 * 
	 * @return
	 */
	ModelTypeEnum getModelType();
	
	//TODO change this to startModel() 
	/**
	 * 
	 */
	void startServer();
	
	// need to change this to model client
	// I dont know if I should give this or just have a ask
	//public PyTranslator getClient();
	
	/**
	 * 
	 * @param question
	 * @param context
	 * @param insight
	 * @param parameters
	 * @return
	 */
	Map<String, String> ask(String question, String context, Insight insight, Map <String, Object> parameters);
	
	/**
	 * 
	 * @param question
	 * @param insight
	 * @param parameters
	 * @return
	 */
	Object embeddings(String question, Insight insight, Map <String, Object> parameters);
	
}
