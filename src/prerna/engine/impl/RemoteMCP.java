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

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.client.transport.customizer.McpSyncHttpClientRequestCustomizer;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpSchema.AudioContent;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.ClientCapabilities;
import io.modelcontextprotocol.spec.McpSchema.Content;
import io.modelcontextprotocol.spec.McpSchema.EmbeddedResource;
import io.modelcontextprotocol.spec.McpSchema.ImageContent;
import io.modelcontextprotocol.spec.McpSchema.InitializeResult;
import io.modelcontextprotocol.spec.McpSchema.ListPromptsResult;
import io.modelcontextprotocol.spec.McpSchema.ListResourceTemplatesResult;
import io.modelcontextprotocol.spec.McpSchema.ListResourcesResult;
import io.modelcontextprotocol.spec.McpSchema.ListToolsResult;
import io.modelcontextprotocol.spec.McpSchema.ResourceLink;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.om.Insight;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;

public class RemoteMCP implements IMCP {

	private static final Logger classLogger = LogManager.getLogger(RemoteMCP.class);

	private String endpoint = null;
	private String authorization = null;

	/**
	 * The local engine/project that owns this connection. Null when the MCP is not
	 * bound to a catalog entry, in which case no identity is stamped onto the tool
	 * listing.
	 */
	private final IEngine engine;

	private String engineId = null;
	private String engineName = null;
	private String engineType = null;

	private McpSyncClient client = null;
	private McpClientTransport transport = null;
	private ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * Connects to an unauthenticated remote MCP. Requests carry no Authorization
	 * header. Use {@link #RemoteMCP(String, String)} for an endpoint that requires
	 * a credential.
	 *
	 * @param endpoint url of the remote MCP server
	 */
	public RemoteMCP(String endpoint) {
		this(endpoint, null, null);
	}

	/**
	 * Connects to a remote MCP using bearer authentication. Use
	 * {@link #RemoteMCP(String, String, String)} for any other scheme.
	 *
	 * @param endpoint  url of the remote MCP server
	 * @param authToken optional credential sent on every request as "Bearer
	 *                  {authToken}". When null or blank no Authorization header is
	 *                  sent.
	 */
	public RemoteMCP(String endpoint, String authToken) {
		this(endpoint, "Bearer", authToken);
	}

	/**
	 * Connects to a remote MCP, optionally authenticating every request.
	 *
	 * @param endpoint   url of the remote MCP server
	 * @param authScheme optional authentication scheme for the Authorization
	 *                   header, such as "Bearer" or "Basic". Defaults to "Bearer"
	 *                   when left blank and a token is provided.
	 * @param authToken  optional credential sent on every request to the remote
	 *                   MCP. When null or blank no Authorization header is sent.
	 */
	public RemoteMCP(String endpoint, String authScheme, String authToken) {
		this(null, endpoint, authScheme, authToken);
	}

	/**
	 * Connects to a remote MCP on behalf of a local engine/project. Binding the
	 * engine lets the tool listing carry the same identity an {@link InternalMCP}
	 * publishes, so callers can route a tool call back to this project.
	 *
	 * @param engine     the local engine/project that owns this connection
	 * @param endpoint   url of the remote MCP server
	 * @param authScheme optional authentication scheme for the Authorization
	 *                   header, such as "Bearer" or "Basic". Defaults to "Bearer"
	 *                   when left blank and a token is provided.
	 * @param authToken  optional credential sent on every request to the remote
	 *                   MCP. When null or blank no Authorization header is sent.
	 */
	public RemoteMCP(IEngine engine, String endpoint, String authScheme, String authToken) {
		this.engine = engine;
		this.endpoint = endpoint;
		this.authorization = buildAuthorizationHeader(authScheme, authToken);
		if (engine != null) {
			this.engineId = engine.getEngineId();
			this.engineName = engine.getEngineName();
			this.engineType = engine.getCatalogType().name();
		}
	}

	private static String buildAuthorizationHeader(String authScheme, String authToken) {
		if (authToken == null || authToken.isBlank()) {
			return null;
		}
		String scheme = (authScheme == null || authScheme.isBlank()) ? "Bearer" : authScheme.trim();
		return scheme + " " + authToken.trim();
	}

	private void connect() {
		if (client == null) {
			// the sdk keeps the origin and the path separately. Handing the whole url to
			// builder() only sets the origin and silently leaves the path at its /mcp
			// default, so every call 404s against an endpoint mounted anywhere else
			URI uri = parseEndpoint();
			HttpClientStreamableHttpTransport.Builder transportBuilder = HttpClientStreamableHttpTransport
					.builder(uri.getScheme() + "://" + uri.getRawAuthority());
			String path = getEndpointPath(uri);
			if (path != null) {
				transportBuilder.endpoint(path);
			}
			if (authorization != null) {
				McpSyncHttpClientRequestCustomizer authCustomizer = (requestBuilder, method, requestUri, body,
						context) -> requestBuilder.header("Authorization", authorization);
				transportBuilder.httpRequestCustomizer(authCustomizer);
			}
			transport = transportBuilder.build();
			client = McpClient.sync(transport).requestTimeout(Duration.ofSeconds(60))
					.capabilities(ClientCapabilities.builder().roots(true).build()).build();
		}
	}

	/**
	 * @return the configured endpoint as a url
	 * @throws IllegalArgumentException if it is not an absolute url, since the
	 *                                  value can be hand edited into the smss
	 */
	private URI parseEndpoint() {
		String trimmed = endpoint == null ? "" : endpoint.trim();
		URI uri = null;
		try {
			uri = new URI(trimmed);
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("The remote MCP endpoint is not a valid url: " + trimmed, e);
		}
		if (uri.getScheme() == null || uri.getRawAuthority() == null) {
			throw new IllegalArgumentException("The remote MCP endpoint must be an absolute url: " + trimmed);
		}
		return uri;
	}

	/**
	 * @return the path, with any query string, that the sdk should send to, or null
	 *         when the url carries no path of its own and the sdk default should
	 *         stand
	 */
	private static String getEndpointPath(URI uri) {
		String path = uri.getRawPath();
		boolean noPath = (path == null || path.isEmpty() || "/".equals(path));
		if (uri.getRawQuery() == null) {
			return noPath ? null : path;
		}
		return (noPath ? "/" : path) + "?" + uri.getRawQuery();
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
			JSONObject toolMap = new JSONObject(objectMapper.writeValueAsString(ltr));
			addEngineMeta(toolMap);
			forceAutoExecutionWithNoUI(toolMap);
			return toolMap;
		} catch (JsonProcessingException e) {
			classLogger.error("Failed to serialize MCP tools for endpoint {}", endpoint, e);
		}
		return null;
	}

	/**
	 * Marks every remote tool as auto-executing with no UI.
	 *
	 * <p>
	 * A remote server picks its own execution mode and may point
	 * {@link MCPUtility#SMSS_MCP_UI} at a portal inside its own project. That
	 * portal does not exist in this installation, so honoring the value would send
	 * the client to a resource that is not there. Nothing a remote tool references
	 * is servable locally, so the UI block is emptied outright rather than
	 * filtered.
	 *
	 * @param toolMap the serialized tools/list result, modified in place
	 */
	private static void forceAutoExecutionWithNoUI(JSONObject toolMap) {
		JSONArray tools = toolMap.optJSONArray("tools");
		if (tools == null) {
			return;
		}
		for (int toolIndex = 0; toolIndex < tools.length(); toolIndex++) {
			JSONObject tool = tools.optJSONObject(toolIndex);
			if (tool == null) {
				continue;
			}
			JSONObject toolMeta = tool.optJSONObject("_meta");
			if (toolMeta == null) {
				toolMeta = new JSONObject();
			}
			toolMeta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPExecution.AUTO.getValue());
			toolMeta.put(MCPUtility.SMSS_MCP_UI, new JSONObject());
			tool.put("_meta", toolMeta);
		}
	}

	/**
	 * Stamps the owning engine's identity onto the tool listing, matching what
	 * {@link InternalMCP#getMCPTools()} publishes, so a caller can route a tool
	 * call back to this project.
	 *
	 * <p>
	 * Any identity the remote server sent describes its own catalog rather than
	 * ours, so those keys are overwritten instead of trusted. Getting this wrong is
	 * not harmless: a remote SEMOSS publishes real project ids, and if one happens
	 * to also exist locally the call would be routed to that unrelated project.
	 * Other keys the remote sent are left alone.
	 *
	 * @param toolMap the serialized tools/list result, modified in place
	 */
	private void addEngineMeta(JSONObject toolMap) {
		if (this.engine == null) {
			return;
		}
		JSONObject _meta = toolMap.optJSONObject("_meta");
		if (_meta == null) {
			_meta = new JSONObject();
		}
		_meta.put(MCPUtility.SMSS_PROJECT_ID, this.engineId);
		_meta.put(MCPUtility.SMSS_PROJECT_NAME, this.engineName);
		_meta.put(MCPUtility.SMSS_ENGINE_ID, this.engineId);
		_meta.put(MCPUtility.SMSS_ENGINE_NAME, this.engineName);
		_meta.put(MCPUtility.SMSS_ENGINE_TYPE, this.engineType);
		toolMap.put("_meta", _meta);
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
		connectAndInit();
		if (this.engineId != null) {
			// tools are published under this engine's id, so an aggregated caller may
			// hand back a prefixed name. The remote only knows its own name
			toolName = MCPUtility.removeEngineIdFromToolsMethodName(this.engineId, toolName);
		}
		CallToolResult result = client.callTool(new CallToolRequest(toolName, params, new HashMap<>()));
		try {
			Content callToolContent = result.content().getFirst();
			String type = callToolContent.type();
			Object value = null;
			if ("text".equals(type)) {
				value = ((TextContent) callToolContent).text();
			} else if ("image".equals(type)) {
				value = ((ImageContent) callToolContent).data();
			} else if ("audio".equals(type)) {
				value = ((AudioContent) callToolContent).data();
			} else if ("resource".equals(type)) {
				value = ((EmbeddedResource) callToolContent).resource().uri();
			} else if ("resource_link".equals(type)) {
				value = ((ResourceLink) callToolContent).uri();
			}

			return value;
		} catch (Exception e) {
			classLogger.error("Failed to parse details from MCP tool result for tool {} on endpoint {}", toolName,
					endpoint, e);
		}
		return null;
	}

}
