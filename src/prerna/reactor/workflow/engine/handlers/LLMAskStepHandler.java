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
package prerna.reactor.workflow.engine.handlers;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
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
			if (!SecurityEngineUtils.userCanViewEngine(insight.getUser(), modelId)) {
				return StepResult.error(stepId, "User does not have access to model engine: " + modelId,
						System.currentTimeMillis() - start);
			}
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
					.withText(userPrompt, userPrompt)
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
