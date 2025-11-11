package prerna.engine.api;

import java.util.Map;

import org.json.JSONObject;

import prerna.om.Insight;

/**
 * This interface is a marker for classes that represent an MCP (Model Context
 * Protocol) resource. Implementing this interface indicates that the class can
 * be managed and interacted with as an MCP resource.
 */
public interface IMCP {

	/**
	 * Initializes the MCP with a specified protocol version and raw message.
	 * 
	 * @param protocolVersion The version of the protocol to use for initialization.
	 * @return A JSONObject indicating the success or failure of the initialization.
	 */
	public JSONObject initMCP(String protocolVersion);

	/**
	 * Retrieves MCP resources based on the raw message.
	 * 
	 * @return A JSONObject containing the MCP resources.
	 */
	public JSONObject getMCPResources();

	/**
	 * Retrieves MCP resources templates based on the raw message
	 * 
	 * @return A JSONObject containing the MCP resources templates.
	 */
	public JSONObject getMCPResourcesTemplates();

	/**
	 * Retrieves MCP prompts based on the raw message.
	 * 
	 * @return A JSONObject containing the MCP prompts.
	 */
	public JSONObject getMCPPrompts();

	/**
	 * Retrieves MCP tools based on the raw message.
	 * 
	 * @return A JSONObject containing the MCP tools.
	 */
	public JSONObject getMCPTools();

	/**
	 * Calls a specific MCP tool with the given function name and parameters.
	 * 
	 * @param toolName The name of the tool to call within the MCP tool.
	 * @param params   A map of parameters to pass to the MCP tool function.
	 * @param insight  The insight executing the tool
	 * @return The object returned by the MCP tool function.
	 */
	public Object callTool(String toolName, Map<String, Object> params, Insight insight);

}
