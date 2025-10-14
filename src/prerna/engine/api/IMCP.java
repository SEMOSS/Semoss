package prerna.engine.api;

import org.json.JSONObject;

public interface IMCP {

	public JSONObject getMCPResources(String rawMessage);
	
	public JSONObject getMCPTools(String rawMessage);
	
	public JSONObject initMCP(String protocolVersion, String rawMessage);
	
	public JSONObject getMCPPrompts(String rawMessage);		

	
	
}
