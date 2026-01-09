/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
