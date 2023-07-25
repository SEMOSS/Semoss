package prerna.engine.api;

import java.util.Map;

import prerna.om.Insight;

public interface IModelEngine {

	// main class that is responsible for controlling everything models
	// hosting modes - embedded, inference_engine, FastChat, OpenAI
	// start server
	// connect client
	// disconnect client
	// stop server ?
	
	// reactors
	// ModelDeployMatchFinder - finds it based on GPU memory etc. 
	// StopModel
	// 
	
	public void loadModel(String modelSmss);
	
	public void startServer();
	
	// need to change this to model client
	// I dont know if I should give this or just have a ask
	//public PyTranslator getClient();
	
	// ask 
	public String ask(String question, String context, Insight insight, Map <String, Object> parameters);
	
	public void stopModel();
	
	public String getCatalogType();
	
	
	
}
