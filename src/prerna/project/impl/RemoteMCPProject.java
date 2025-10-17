package prerna.project.impl;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
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
import io.modelcontextprotocol.spec.McpSchema.ListPromptsResult;
import io.modelcontextprotocol.spec.McpSchema.ListResourcesResult;
import io.modelcontextprotocol.spec.McpSchema.ListToolsResult;
import io.modelcontextprotocol.spec.McpSchema.Root;
import prerna.engine.api.IMCP;
import prerna.reactor.agent.mcp.GetMCPInternalToolsReactor;

public class RemoteMCPProject extends Project {

	String bearerToken = null;	
	private static final Logger classLogger = LogManager.getLogger(GetMCPInternalToolsReactor.class);
	static String clientName = "Semoss";
	static long id = 1;	

	McpSyncClient client = null;
	String endpoint = null;
	McpClientTransport transport = null;
	ObjectMapper objectMapper = new ObjectMapper();
	enum Transport {STDIO, HTTP};
	public static String MCP_ENDPOINT = "MCP_ENDPOINT";
	
	
	public void connect()
	{
		connect(RemoteMCPClient.Transport.HTTP);
	}
	
	public void connect(RemoteMCPClient.Transport transportMode)
	{
		if(client == null)
		{
			if(transportMode != null && transportMode == RemoteMCPClient.Transport.HTTP)
			{
				endpoint = this.getSmssProp().getProperty(MCP_ENDPOINT);
				transport = HttpClientStreamableHttpTransport
		                .builder(endpoint)
		                .build();
			}
			
			client = McpClient.sync((McpClientTransport) transport)
				    .requestTimeout(Duration.ofSeconds(10))
				    .capabilities(ClientCapabilities.builder()
				        .roots(true)      // Enable roots capability
				        .sampling()       // Enable sampling capability
				        .elicitation()    // Enable elicitation capability
				        .build())
				   // .sampling(request -> CreateMessageResult.builder()...build())
				   // .elicitation(elicitRequest -> ElicitResult.builder()...build())
				   // .toolsChangeConsumer((List<McpSchema.Tool> tools) -> ...)
				   // .resourcesChangeConsumer((List<McpSchema.Resource> resources) -> ...)
				   // .promptsChangeConsumer((List<McpSchema.Prompt> prompts) -> ...)
				   // .loggingConsumer((LoggingMessageNotification logging) -> ...)
				   // .progressConsumer((ProgressNotification progress) -> ...)
				    .build();
		}
	}
	
	public JSONObject getMCPResources(String inputJson)
	{
		connect();
		ListResourcesResult lrr = client.listResources();
		
		// compose string
		try {
			return new JSONObject(objectMapper.writeValueAsString(lrr));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public JSONObject getMCPPrompts(String inputJson)
	{
		connect();
		ListPromptsResult lpr = client.listPrompts();
		
		// compose string
		try {
			return new JSONObject(objectMapper.writeValueAsString(lpr));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public JSONObject getMCPTools(String inputJson)
	{
		connect();
		ListToolsResult ltr = client.listTools();
		
		// compose string
		try {
			return new JSONObject(objectMapper.writeValueAsString(ltr));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Object callTool(String inputJson)
	{
		connect();
		JSONObject json = new JSONObject(inputJson);
		JSONObject toolParams = json.getJSONObject("params");
		String toolName = toolParams.getString("name");
		JSONObject methodParams = toolParams.getJSONObject("arguments");
		Iterator jsonKeys = methodParams.keys();
		Map params = new HashMap();
		while(jsonKeys.hasNext())
		{
			Object key = jsonKeys.next();
			Object value = methodParams.get(key+"");
			params.put(key, value);
		}
		
		// Call a tool
		CallToolResult result = client.callTool(
		    new CallToolRequest(toolName,
		        params)
		);
		System.err.println(result.structuredContent());
		
		// {"jsonrpc":"2.0","id":5,"result":{"content":[{"type":"text","text":"334.07000732421875"}],"isError":false}}
		try {
			JSONObject contentObj = new JSONObject(objectMapper.writeValueAsString(result.structuredContent()));
			System.err.println(contentObj);
			return objectMapper.writeValueAsString(contentObj.get("result"));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
	
	public static void main(String [] args)
	{
		IMCP rmc = new RemoteMCPClient();
		((RemoteMCPClient)rmc).endpoint = "http://localhost:8000/mcp";
		((RemoteMCPClient)rmc).connect(RemoteMCPClient.Transport.HTTP);
	
		String toolJson = " {\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\",\"params\":{\"name\":\"greet\",\"arguments\":{\"name\":\"friend\"}}}";
		
		System.err.println(rmc.getMCPResources(null));		
		System.err.println(rmc.getMCPPrompts(null));		
		System.err.println(rmc.getMCPTools(null));		
		
		String output = rmc.callTool(toolJson) + "";
		System.err.println(output);
	}
	
	
	public static void main2(String[] args) {
		// TODO Auto-generated method stub
		// Create a sync client with custom configuration
		McpClientTransport transport = HttpClientStreamableHttpTransport
                .builder("http://localhost:8000/mcp")
                .build();
		
		
		
		McpSyncClient client = McpClient.sync((McpClientTransport) transport)
		    .requestTimeout(Duration.ofSeconds(10))
		    .capabilities(ClientCapabilities.builder()
		        .roots(true)      // Enable roots capability
		        .sampling()       // Enable sampling capability
		        .elicitation()    // Enable elicitation capability
		        .build())
		   // .sampling(request -> CreateMessageResult.builder()...build())
		   // .elicitation(elicitRequest -> ElicitResult.builder()...build())
		   // .toolsChangeConsumer((List<McpSchema.Tool> tools) -> ...)
		   // .resourcesChangeConsumer((List<McpSchema.Resource> resources) -> ...)
		   // .promptsChangeConsumer((List<McpSchema.Prompt> prompts) -> ...)
		   // .loggingConsumer((LoggingMessageNotification logging) -> ...)
		   // .progressConsumer((ProgressNotification progress) -> ...)
		    .build();

		// Initialize connection
		client.initialize();

		// List available tools
		ListToolsResult tools = client.listTools();

		//String json = " {\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\",\"params\":{\"name\":\"get_stock_price\",\"arguments\":{\"symbol\":\"GOOG\", \"bruh\":\"me\"}}}";
		String json = " {\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\",\"params\":{\"name\":\"greet\",\"arguments\":{\"name\":\"prabhu\"}}}";
		JSONObject inputJson = new JSONObject(json);
		JSONObject toolParams = inputJson.getJSONObject("params");
		String toolName = toolParams.getString("name");
		JSONObject methodParams = toolParams.getJSONObject("arguments");
		Iterator jsonKeys = methodParams.keys();
		Map params = new HashMap();
		while(jsonKeys.hasNext())
		{
			Object key = jsonKeys.next();
			Object value = methodParams.get(key+"");
			params.put(key, value);
		}
		
		// Call a tool
		CallToolResult result = client.callTool(
		    new CallToolRequest(toolName,
		        params)
		);
		
		System.err.println(result.structuredContent());

		// List and read resources
		ListResourcesResult resources = client.listResources();
		System.err.println(resources);
		
		// List and use prompts
		ListPromptsResult prompts = client.listPrompts();
		System.err.println(prompts);

//		GetPromptResult prompt = client.getPrompt(
//		    new GetPromptRequest("greeting", Map.of("name", "Spring"))
//		);

		// Add/remove roots
		client.addRoot(new Root("file:///path", "description"));
		client.removeRoot("file:///path");

		// Close client
		client.closeGracefully();

	}

	
	
}
