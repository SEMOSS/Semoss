package prerna.reactor.workflow.engine.handlers;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageType;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.om.Insight;
import prerna.reactor.workflow.engine.StepResult;
import prerna.reactor.workflow.engine.WorkflowContext;
import prerna.util.Utility;

/**
 * Sends a single prompt to an LLM and returns the text response.
 * No tool calling — for agentic behavior use LLM_AGENT instead.
 * 
 * Config:
 *   modelId      — the model engine ID
 *   systemPrompt — system prompt (optional)
 *   userPrompt   — the user message to send
 *   paramMap     — optional model parameters (temperature, etc.)
 */
public class LLMAskStepHandler implements IWorkflowStepHandler {

	private static final Logger classLogger = LogManager.getLogger(LLMAskStepHandler.class);

	@SuppressWarnings("unchecked")
	@Override
	public StepResult execute(String stepId, Map<String, Object> config,
			WorkflowContext context, Insight insight) {
		long start = System.currentTimeMillis();

		String modelId = (String) config.get("modelId");
		String systemPrompt = (String) config.get("systemPrompt");
		String userPrompt = (String) config.get("userPrompt");
		Map<String, Object> paramMap = (Map<String, Object>) config.get("paramMap");

		if (modelId == null || modelId.isEmpty()) {
			return StepResult.error(stepId, "LLM_ASK step requires 'modelId' in config",
					System.currentTimeMillis() - start);
		}
		if (userPrompt == null || userPrompt.isEmpty()) {
			return StepResult.error(stepId, "LLM_ASK step requires 'userPrompt' in config",
					System.currentTimeMillis() - start);
		}

		try {
			IModelEngine modelEngine = (IModelEngine) Utility.getEngine(modelId);
			if (modelEngine == null) {
				return StepResult.error(stepId, "Model engine not found: " + modelId,
						System.currentTimeMillis() - start);
			}

			// Create a transient room for this step
			Room room = new Room();
			room.setInsight(insight);

			InputMessage msg = InputMessage.builder(room)
					.withSystemPrompt(systemPrompt)
					.withInputUIPrompt(userPrompt)
					.withInputPrompt(userPrompt)
					.withModelType(modelEngine.getModelType())
					.withParamMap(paramMap)
					.build();

			ResponseMessage response = room.ask(msg, modelEngine);

			if (response.getMessageType() == MessageType.RESPONSE_TEXT) {
				String text = response.getContent();
				StepResult result = StepResult.success(stepId, text, System.currentTimeMillis() - start);
				result.getMetadata().put("messageId", response.getMessageId());
				return result;
			} else {
				// Unexpected response type (e.g., tool call with no tools attached)
				return StepResult.error(stepId,
						"LLM returned unexpected response type: " + response.getMessageType(),
						System.currentTimeMillis() - start);
			}
		} catch (Exception e) {
			classLogger.error("LLM_ASK step '{}' failed", stepId, e);
			return StepResult.error(stepId, "LLM ask failed: " + e.getMessage(),
					System.currentTimeMillis() - start);
		}
	}
}
