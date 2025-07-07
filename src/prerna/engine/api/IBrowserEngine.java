package prerna.engine.api;

import org.json.JSONObject;

public interface IBrowserEngine extends IEngine {

	String getBrowserFile();
	
	JSONObject getBrowserFileInstructions();
	
}
