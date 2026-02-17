package prerna.reactor.workflow.engine.handlers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.MCPFactory;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;
import prerna.reactor.workflow.engine.StepResult;
import prerna.reactor.workflow.engine.WorkflowContext;
import prerna.util.Utility;
import prerna.reactor.agent.mcp.MCPUtility;

/**
 * Full agentic LLM loop: sends a prompt with tools, handles tool calls iteratively
 * until the model returns a text response or max iterations are reached.
 * 
 * Config:
 *   modelId        - the model engine ID
 *   systemPrompt   - system prompt (optional)
 *   userPrompt     - the user message to send
 *   toolEngineIds  - list of engine/project IDs whose MCP tools are available
 *   paramMap       - optional model parameters (temperature, etc.)
 *   maxIterations  - max tool-call rounds before forcing stop (default: 10)
 */
public class LLMAgentStepHandler implements IWorkflowStepHandler {

	private static final Logger classLogger = LogManager.getLogger(LLMAgentStepHandler.class);
	private static final int DEFAULT_MAX_ITERATIONS = 10;

	@SuppressWarnings("unchecked")
	@Override
	public StepResult execute(String stepId, Map<String, Object> config,
			WorkflowContext context, Insight insight) {
		long start = System.currentTimeMillis();

		String modelId = (String) config.get("modelId");
		String systemPrompt = (String) config.get("systemPrompt");
		String userPrompt = (String) config.get("userPrompt");
		List<String> toolEngineIds = (List<String>) config.get("toolEngineIds");
		Map<String, Object> paramMap = (Map<String, Object>) config.get("paramMap");
		int maxIterations = config.containsKey("maxIterations")
				? ((Number) config.get("maxIterations")).intValue()
				: DEFAULT_MAX_ITERATIONS;

		if (modelId == null || modelId.isEmpty()) {
			return StepResult.error(stepId, "LLM_AGENT step requires 'modelId' in config",
					System.currentTimeMillis() - start);
		}
		if (userPrompt == null || userPrompt.isEmpty()) {
			return StepResult.error(stepId, "LLM_AGENT step requires 'userPrompt' in config",
					System.currentTimeMillis() - start);
		}

		try {
			IModelEngine modelEngine = (IModelEngine) Utility.getEngine(modelId);
			if (modelEngine == null) {
				return StepResult.error(stepId, "Model engine not found: " + modelId,
						System.currentTimeMillis() - start);
			}

			// Gather MCP tool definitions from all specified tool engines
			List<Map<String, Object>> allTools = new ArrayList<>();
			Map<String, IMCP> toolNameToMcp = new HashMap<>();
			if (toolEngineIds != null) {
				for (String engineId : toolEngineIds) {
					IEngine engine = Utility.getEngine(engineId);
					if (engine == null) {
						engine = Utility.getProject(engineId);
					}
					if (engine != null) {
						IMCP mcp = MCPFactory.build(engine);
						List<Map<String, Object>> tools = getToolsFromEngine(engine, engineId);
						for (Map<String, Object> tool : tools) {
							Map<String, Object> fn = (Map<String, Object>) tool.get("function");
							if (fn != null) {
								String name = (String) fn.get("name");
								if (name != null) {
									toolNameToMcp.put(name, mcp);
								}
							}
						}
						allTools.addAll(tools);
					}
				}
			}

			// Create room and build initial message
			Room room = new Room();
			room.setInsight(insight);

			InputMessage msg = InputMessage.builder(room)
					.withSystemPrompt(systemPrompt)
					.withInputUIPrompt(userPrompt)
					.withInputPrompt(userPrompt)
					.withModelType(modelEngine.getModelType())
					.withParamMap(paramMap)
					.withTools(allTools)
					.build();

			ResponseMessage response = room.ask(msg, modelEngine);
			int iterations = 0;

			// Agent loop: handle tool calls until text response or max iterations
			while (response.getMessageType() == MessageType.RESPONSE_TOOL && iterations < maxIterations) {
				iterations++;

				List<Map<String, Object>> toolCalls = response.getToolResponses();
				if (toolCalls == null || toolCalls.isEmpty()) break;

				AskModelEngineResponse<?> lastAskResponse = null;

				for (Map<String, Object> toolCall : toolCalls) {
					String toolCallId = String.valueOf(toolCall.get("id"));
					Map<String, Object> function = (Map<String, Object>) toolCall.get("function");
					String toolName = (String) function.get("name");
					Map<String, Object> toolParams = (Map<String, Object>) function.get("arguments");

					// Execute the tool via the mapped MCP instance
					Object toolOutput = null;
					String toolStatus = "success";
					IMCP mcp = toolNameToMcp.get(toolName);
					if (mcp != null) {
						try {
							toolOutput = mcp.callTool(toolName, toolParams, insight);
						} catch (Exception e) {
							toolOutput = "Error: " + e.getMessage();
							toolStatus = "error";
						}
					} else {
						toolOutput = "No MCP handler found for tool: " + toolName;
						toolStatus = "error";
					}

					String resultStr = toolOutput != null ? toolOutput.toString() : "No output";

					// Feed tool result back to the room
					lastAskResponse = room.addToolExecutionResult(
							toolCallId, toolName, resultStr, toolParams,
							paramMap, null, modelEngine, insight, toolStatus);
				}

				// addToolExecutionResult re-asks the model when all tool calls are fulfilled
				if (lastAskResponse != null) {
					response = ResponseMessage.Builder
							.fromAskModelEngineResponse(lastAskResponse).build();
				} else {
					break;
				}
			}

			if (response.getMessageType() == MessageType.RESPONSE_TEXT) {
				String text = response.getContent();
				StepResult result = StepResult.success(stepId, text, System.currentTimeMillis() - start);
				result.getMetadata().put("iterations", iterations);
				result.getMetadata().put("messageId", response.getMessageId());
				return result;
			} else {
				return StepResult.error(stepId,
						"Agent loop exhausted after " + maxIterations + " iterations without a text response",
						System.currentTimeMillis() - start);
			}
		} catch (Exception e) {
			classLogger.error("LLM_AGENT step '{}' failed", stepId, e);
			return StepResult.error(stepId, "LLM agent failed: " + e.getMessage(),
					System.currentTimeMillis() - start);
		}
	}

	/**
	 * Load tool definitions from an engine in the List of Map format expected by InputMessage.withTools().
	 */
	private List<Map<String, Object>> getToolsFromEngine(IEngine engine, String engineId) {
		JSONObject toolMap = MCPUtility.getAggregatedTools(engine);
		JSONObject updatedToolMap = MCPUtility.appendEngineIdToToolsMethodName(engineId, toolMap);
		if (updatedToolMap != null && updatedToolMap.has("tools")) {
			JSONArray arr = updatedToolMap.getJSONArray("tools");
			List<Map<String, Object>> result = new ArrayList<>();
			for (int i = 0; i < arr.length(); i++) {
				JSONObject toolObj = arr.optJSONObject(i);
				if (toolObj != null) {
					result.add(toolObj.toMap());
				}
			}
			return result;
		}
		return Collections.emptyList();
	}
}
