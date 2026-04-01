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
package prerna.reactor.shortcuts.temporal;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import prerna.om.Insight;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class WorkflowActivityImpl implements WorkflowActivity {

	@Override
	public WorkflowDefinition loadDefinition(String workflowKey) {
		try {
			WorkflowEntity entity = SchedulerDatabaseUtility.getWorkflowByKey(workflowKey);
			if (entity == null) {
				throw new RuntimeException("Workflow not found: " + workflowKey);
			}

			return JsonUtil.fromJson(entity.workflowJson, WorkflowDefinition.class);
		} catch (Exception e) {
			throw new RuntimeException("Failed to load workflow definition for: " + workflowKey, e);
		}
	}

	@Override
	public long saveWorkflowExecution(String workflowKey, String triggerType, String filePath, String status) {
		String workflowId = io.temporal.activity.Activity.getExecutionContext().getInfo().getWorkflowId();
		try {
			return SchedulerDatabaseUtility.insertWorkflowExecution("EXEC_" + UUID.randomUUID(), workflowKey,
					workflowId, triggerType, filePath, status, filePath);
		} catch (Exception e) {
			throw new RuntimeException("Failed to save workflow execution for: " + workflowKey, e);
		}
	}

	@Override
	public void executeWorkflow(WorkflowDefinition definition, long executionId, String workflowKey, String filePath,
			String insightId) {

		if (definition == null) {
			throw new RuntimeException("Workflow definition is null");
		}

		Insight insight = new Insight();
		insight.setInsightId(insightId);

		try {
			// Extract filename with extension from filePath
			String filename = new java.io.File(filePath).getName();

			// Execute workflow steps according to flow
			java.util.Set<String> actionsToSkip = new java.util.HashSet<>();

			for (List<String> step : definition.flow) {
				// Process all actions in this step
				for (String actionId : step) {
					// Skip if this action should be skipped due to routing
					if (actionsToSkip.contains(actionId)) {
						continue;
					}

					// Find action
					Action action = findAction(definition, actionId);

					if (action == null) {
						continue; // Skip missing actions
					}

					try {
						// Replace {filename} with extracted filename
						String pixelToExecute = action.pixel;
						if (pixelToExecute.contains("{filename}")) {
							pixelToExecute = pixelToExecute.replace("{filename}", filename);
						}

						// Execute based on action type
						if ("CONDITION".equals(action.actionType)) {
							// Handle CONDITION/ROUTE action
							PixelRunner pixelRunner = insight.runPixel(pixelToExecute);

							if (!pixelRunner.getResults().isEmpty()) {
								Map<String, Object> conditionResult = (Map<String, Object>) pixelRunner.getResults()
										.get(0).getValue();

								// Extract which action should be executed based on condition result
								String nextActionId = evaluateCondition(action, conditionResult);

								// Mark sibling actions in the NEXT step as skipped (not future steps)
								int currentStepIdx = definition.flow.indexOf(step);
								if (nextActionId != null && currentStepIdx + 1 < definition.flow.size()) {
									List<String> nextStep = definition.flow.get(currentStepIdx + 1);
									for (String sibling : nextStep) {
										if (!sibling.equals(nextActionId)) {
											actionsToSkip.add(sibling);
										}
									}
								}
							}

							// Store condition result
							String resultKey = extractResultKey(action.pixel);
							if (resultKey != null && !pixelRunner.getResults().isEmpty()) {
								NounMetadata nounMetadata = pixelRunner.getResults().get(0);
								insight.getVarStore().put(resultKey, nounMetadata);
							}

							// Log success
							SchedulerDatabaseUtility.insertActionExecution(executionId, action.actionId,
									action.actionName, "SUCCESS", action.pixel, "", null, null);

						} else {
							// Handle regular ACTION (SYNC or ASYNC)
							PixelRunner pixelRunner = insight.runPixel(pixelToExecute);

							// Extract resultKey from pixel config
							String resultKey = extractResultKey(action.pixel);

							// Store result using resultKey if available
							if (resultKey != null && !pixelRunner.getResults().isEmpty()) {
								NounMetadata nounMetadata = pixelRunner.getResults().get(0);
								insight.getVarStore().put(resultKey, nounMetadata);
							}

							// Log success
							SchedulerDatabaseUtility.insertActionExecution(executionId, action.actionId,
									action.actionName, "SUCCESS", action.pixel, "", null, null);
						}

					} catch (Exception ex) {
						// Log failure
						SchedulerDatabaseUtility.insertActionExecution(executionId, action.actionId, action.actionName,
								"FAILED", action.pixel, "", null, ex.getMessage());

						// Check if we should continue on failure
						if (definition.execution != null && !definition.execution.continueOnFailure) {
							throw ex;
						}
					}
				}
			}

		} catch (Exception e) {
			throw new RuntimeException("Workflow execution error: " + e.getMessage(), e);
		}
	}

	private Action findAction(WorkflowDefinition definition, String actionId) {
		return definition.actions.stream()
				.filter(a -> a.actionId.equals(actionId))
				.findFirst()
				.orElse(null);
	}

	private String evaluateCondition(Action conditionAction, Map<String, Object> conditionResult) {
		try {
			// ConditionReactor returns {nextNode=action5} in the result
			if (conditionResult != null && conditionResult.containsKey("nextNode")) {
				Object nextNodeObj = conditionResult.get("nextNode");
				if (nextNodeObj != null) {
					return nextNodeObj.toString();
				}
			}
		} catch (Exception e) {
			// Log and return null to continue with default
		}
		return null;
	}

	private String extractResultKey(String pixelString) {
		try {
			// Extract resultKey from pixel config
			// Format: "Action(config=[{\"resultKey\":\"action1\", ...}]);"
			int start = pixelString.indexOf("\"resultKey\"");
			if (start != -1) {
				// Find the value after "resultKey":
				int colonPos = pixelString.indexOf(":", start);
				if (colonPos != -1) {
					int quoteStart = pixelString.indexOf("\"", colonPos);
					if (quoteStart != -1) {
						int quoteEnd = pixelString.indexOf("\"", quoteStart + 1);
						if (quoteEnd != -1) {
							return pixelString.substring(quoteStart + 1, quoteEnd);
						}
					}
				}
			}
		} catch (Exception e) {
			// Failed to extract resultKey
		}
		return null;
	}

	private PixelRequest buildRequest(Action action, String workflowKey, long executionId, String fileName,
			Map<String, Object> context) {

		PixelRequest req = new PixelRequest();

		req.setWorkflowKey(workflowKey);
		req.setWorkflowExecutionId(executionId);
		req.setActionId(action.actionId);
		req.setActionName(action.actionName);
		req.setPixel(action.pixel != null && action.pixel.contains("{filename}")
				? action.pixel.replace("{filename}", fileName)
				: action.pixel);
		req.setContext(context);

		return req;
	}
}
