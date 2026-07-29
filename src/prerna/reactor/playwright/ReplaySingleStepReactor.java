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
package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ReplaySingleStepReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ReplaySingleStepReactor.class);

	private ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	/**
	 * Default constructor for ReplaySingleStepReactor. Initializes the keys this
	 * reactor expects: projectId, sessionId, fileName, paramValues, stepId, and
	 * tabId.
	 */
	public ReplaySingleStepReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), "sessionId", "fileName",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), "stepId", "tabId", };
		this.keyRequired = new int[] { 1, 1, 1, 0, 1, 0 };
	}

	/**
	 * Executes the reactor to replay a single step from a Playwright recording.
	 *
	 * @return A NounMetadata object containing the result of the step replay,
	 *         including status, any errors, and a screenshot.
	 * @throws IllegalArgumentException If required parameters are missing or
	 *                                  invalid.
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String sessionId = this.keyValue.get(this.keysToGet[1]);
		String fileName = this.keyValue.get(this.keysToGet[2]);
		Map<String, Object> inputs = getMap(this.keysToGet[3]);
		int stepId = Integer.parseInt(this.keyValue.get(this.keysToGet[4]));
		String tabId = this.keyValue.get(this.keysToGet[5]);
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to view the project");
		}

		Map<String, Object> response = replayStep(projectId, sessionId, fileName, stepId, inputs, tabId);

		return new NounMetadata(response, PixelDataType.MAP);
	}

	/**
	 * Replays a single Playwright step from a recorded script.
	 *
	 * @param projectId The ID of the project the recording belongs to.
	 * @param sessionId The ID of the active Playwright session.
	 * @param fileName  The name of the recording file.
	 * @param stepId    The ID of the step to replay.
	 * @param inputs    A map of input values for TYPE steps, where the key is the
	 *                  step label.
	 * @param tabId     The ID of the tab where the step should be replayed.
	 * @return A map containing the status of the replay, any errors, and a
	 *         screenshot after execution.
	 */
	public Map<String, Object> replayStep(String projectId, String sessionId, String fileName, int stepId,
			Map<String, Object> inputs, String tabId) {

		Map<String, Object> response = new HashMap<>();
		try {
			// Load the steps file
			StepsEnvelope env = PlaywrightUtility.loadStepsFromFile(projectId, fileName);
			Map<String, List<List<PlaywrightStep>>> allStepsMap = env.steps();

			// Find the step by ID
			StepLocation location = findStepById(allStepsMap, stepId);

			if (location == null) {
				response.put("status", "failed");
				response.put("error", "Step with ID " + stepId + " not found");
				return response;
			}

			PlaywrightStep step = location.step;
			String actualTabId = location.tabId;

			// If tabId was provided, verify it matches
			if (tabId != null && !tabId.isEmpty() && !tabId.equals(actualTabId)) {
				classLogger.warn("Provided tabId {} doesn't match step's tabId {}", tabId, actualTabId);
			}

			classLogger.info("Found step {} in tab {}: {}", stepId, actualTabId, json.valueToTree(step).toString());

			// Get session
			PlaywrightSession s = this.insight.getUser().getPlaywrightSession(sessionId);
			if (s == null) {
				response.put("status", "failed");
				response.put("error", "Session not found");
				return response;
			}

			// Validate step can be executed
			// String validationError = validateStep(s, step, inputs, actualTabId);
			// if (validationError != null) {
			// response.put("status", "failed");
			// response.put("error", validationError);
			// response.put("stepId", stepId);
			// response.put("tabId", actualTabId);

			// ScreenshotResponse screenshot = ScreenshotReactor.screenshot(s, actualTabId);
			// response.put("screenshot", screenshot);

			// return response;
			// }

			// Execute the step and capture result
			PlaywrightStep stepToExecute = step;
			Object inputValue = resolveInputValue(step, inputs);
			if (step.type() == PlaywrightStepType.TYPE && inputValue != null) {
				stepToExecute = new PlaywrightStep(step, inputValue.toString());
			}

			Map<String, Object> executionResult;
			ScreenshotResponse screenshot;
			s.getOperationLock().lock();
			try {
				executionResult = PlaywrightSessionUtility.applyStep(s, stepToExecute, actualTabId);
				screenshot = ScreenshotReactor.screenshot(s, actualTabId);
			} finally {
				s.getOperationLock().unlock();
			}
			response.put("screenshot", screenshot);

			if (executionResult != null && !"failed".equals(executionResult.get("status"))) {
				response.put("status", "success");
				response.put("stepId", stepId);
				response.put("tabId", actualTabId);
				Boolean shouldStop = (Boolean) executionResult.get("shouldStop");
				response.put("shouldStop", shouldStop);

				// Check if new tab was created
				Boolean isNewTab = (Boolean) executionResult.get("isNewTab");
				String newTabId = (String) executionResult.get("newTabId");
				String tabTitle = (String) executionResult.get("tabTitle");

				if (isNewTab != null && isNewTab) {
					response.put("isNewTab", true);
					response.put("newTabId", newTabId);
					response.put("tabTitle", tabTitle);
					classLogger.info("Step created new tab: {}", newTabId);
				} else {
					response.put("isNewTab", false);
				}

				if (step.type() == PlaywrightStepType.NAVIGATE) {
					response.put("tabTitle", tabTitle);
				}
			} else {
				response.put("status", "failed");
				response.put("error",
						executionResult != null && executionResult.get("error") != null ? executionResult.get("error")
								: "Step execution failed");
				response.put("stepId", stepId);
				response.put("tabId", actualTabId);
				response.put("isNewTab", false);
			}

		} catch (Exception e) {
			classLogger.error("Error replaying step {}", stepId, e);
			response.put("status", "failed");
			response.put("error", e.getMessage());
			response.put("isNewTab", false);

			// Try to get screenshot even on exception
			try {
				PlaywrightSession s = this.insight.getUser().getPlaywrightSession(sessionId);
				if (s != null) {
					String actualTabId = tabId != null && !tabId.isEmpty() ? tabId : "tab-1";
					if (s.getPage(actualTabId) != null) {
						ScreenshotResponse screenshot = ScreenshotReactor.screenshot(s, actualTabId);
						response.put("screenshot", screenshot);
					}
				}
			} catch (Exception screenshotEx) {
				classLogger.error("Failed to capture screenshot after error", screenshotEx);
			}
		}

		return response;
	}

	/**
	 * Resolves a TYPE override using every parameter-key format published by the
	 * Playwright MCP generators. Exact labels remain supported for legacy callers.
	 */
	private static Object resolveInputValue(PlaywrightStep step, Map<String, Object> inputs) {
		if (step.type() != PlaywrightStepType.TYPE || inputs == null || inputs.isEmpty()) {
			return null;
		}
		if (step.label() != null && inputs.containsKey(step.label())) {
			return inputs.get(step.label());
		}
		if (step.label() != null) {
			String sanitizedLabel = PlaywrightMCPToolBuilder.sanitizeToolName(step.label(), "field_");
			if (inputs.containsKey(sanitizedLabel)) {
				return inputs.get(sanitizedLabel);
			}
		}
		String stepKey = "step_" + step.id();
		if (inputs.containsKey(stepKey)) {
			return inputs.get(stepKey);
		}
		String numericKey = String.valueOf(step.id());
		return inputs.get(numericKey);
	}

	/**
	 * Searches through all steps in the {@link StepsEnvelope} to find a specific
	 * step by its ID.
	 *
	 * @param allStepsMap A map where keys are tab IDs and values are lists of
	 *                    pages, each containing a list of {@link PlaywrightStep}s.
	 * @param stepId      The ID of the {@link PlaywrightStep} to find.
	 * @return A {@link StepLocation} object containing the found step and its tab
	 *         ID, or null if the step is not found.
	 */
	private StepLocation findStepById(Map<String, List<List<PlaywrightStep>>> allStepsMap, int stepId) {
		for (Map.Entry<String, List<List<PlaywrightStep>>> entry : allStepsMap.entrySet()) {
			String tabId = entry.getKey();
			List<List<PlaywrightStep>> pages = entry.getValue();

			for (List<PlaywrightStep> page : pages) {
				for (PlaywrightStep step : page) {
					if (step.id() == stepId) {
						return new StepLocation(step, tabId);
					}
				}
			}
		}
		return null;
	}

	/**
	 * A private inner class to hold a {@link PlaywrightStep} and its associated tab
	 * ID.
	 */
	private static class StepLocation {
		PlaywrightStep step;
		String tabId;

		/**
		 * Constructs a new StepLocation.
		 * 
		 * @param step  The {@link PlaywrightStep}.
		 * @param tabId The ID of the tab where the step is located.
		 */
		StepLocation(PlaywrightStep step, String tabId) {
			this.step = step;
			this.tabId = tabId;
		}
	}

	@Override
	public String getReactorDescription() {
		return "Reactor that replays a single step by given stepId , sesionId, tabId, and fileName";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		} else if (key.equals("fileName")) {
			return "the name of the recorder file";
		} else if (key.equals("stepId")) {
			return "The id of the current step that needs to be played";
		} else if (key.equals("tabId")) {
			return "The id of the current tab of the playwright";
		}
		return super.getDescriptionForKey(key);
	}
}
