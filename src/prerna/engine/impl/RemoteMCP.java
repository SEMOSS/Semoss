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
package prerna.engine.impl;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.ClientCapabilities;
import io.modelcontextprotocol.spec.McpSchema.InitializeResult;
import io.modelcontextprotocol.spec.McpSchema.ListPromptsResult;
import io.modelcontextprotocol.spec.McpSchema.ListResourceTemplatesResult;
import io.modelcontextprotocol.spec.McpSchema.ListResourcesResult;
import io.modelcontextprotocol.spec.McpSchema.ListToolsResult;
import prerna.engine.api.IMCP;
import prerna.om.Insight;

public class RemoteMCP implements IMCP {

	private static final Logger classLogger = LogManager.getLogger(RemoteMCP.class);

	private String endpoint = null;

	private McpSyncClient client = null;
	private McpClientTransport transport = null;
	private ObjectMapper objectMapper = new ObjectMapper();

	public RemoteMCP(String endpoint) {
		this.endpoint = endpoint;
	}

	private void connect() {
		if (client == null) {
			transport = HttpClientStreamableHttpTransport.builder(endpoint).build();

			client = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(10))
					.capabilities(ClientCapabilities.builder().roots(true) // Enable roots capability
							.sampling() // Enable sampling capability
							.elicitation() // Enable elicitation
							.build())
					.build();
		}
	}

	private void connectAndInit() {
		connect();
		if (!client.isInitialized()) {
			initMCP(null);
		}
	}

	@Override
	public JSONObject initMCP(String protocolVersion) {
		connect();
		InitializeResult ir = client.initialize();
		try {
			return new JSONObject(objectMapper.writeValueAsString(ir));
		} catch (JsonProcessingException e) {
			classLogger.error("Failed to serialize MCP initialize result for endpoint {}", endpoint, e);
		}
		return null;
	}

	@Override
	public JSONObject getMCPResources() {
		connectAndInit();
		ListResourcesResult lrr = client.listResources();
		try {
			return new JSONObject(objectMapper.writeValueAsString(lrr));
		} catch (JsonProcessingException e) {
			classLogger.error("Failed to serialize MCP resources for endpoint {}", endpoint, e);
		}
		return null;
	}

	@Override
	public JSONObject getMCPResourcesTemplates() {
		connectAndInit();
		ListResourceTemplatesResult lrtr = client.listResourceTemplates();
		try {
			return new JSONObject(objectMapper.writeValueAsString(lrtr));
		} catch (JsonProcessingException e) {
			classLogger.error("Failed to serialize MCP resource templates for endpoint {}", endpoint, e);
		}
		return null;
	}

	@Override
	public JSONObject getMCPTools() {
		connectAndInit();
		ListToolsResult ltr = client.listTools();
		try {
			return new JSONObject(objectMapper.writeValueAsString(ltr));
		} catch (JsonProcessingException e) {
			classLogger.error("Failed to serialize MCP tools for endpoint {}", endpoint, e);
		}
		return null;
	}

	@Override
	public JSONObject getMCPPrompts() {
		connectAndInit();
		ListPromptsResult lpr = client.listPrompts();
		try {
			return new JSONObject(objectMapper.writeValueAsString(lpr));
		} catch (JsonProcessingException e) {
			classLogger.error("Failed to serialize MCP prompts for endpoint {}", endpoint, e);
		}
		return null;
	}

	@Override
	public Object callTool(String toolName, Map<String, Object> params, Insight insight) {
		CallToolResult result = client.callTool(new CallToolRequest(toolName, params, new HashMap<>()));
		try {
			JSONObject contentObj = new JSONObject(objectMapper.writeValueAsString(result.structuredContent()));
			return objectMapper.writeValueAsString(contentObj.get("result"));
		} catch (JsonProcessingException e) {
			classLogger.error("Failed to serialize MCP tool result for tool {} on endpoint {}", toolName, endpoint, e);
		}
		return null;
	}

}
