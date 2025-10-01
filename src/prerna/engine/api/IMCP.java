package prerna.engine.api;

import org.json.JSONObject;

public interface IMCP {

	public JSONObject getMCPResources();
	
	public JSONObject getMCPTools();
	
	public JSONObject initMCP(String protocolVersion);
	
	public JSONObject getMCPPrompts();		

	
	
}
