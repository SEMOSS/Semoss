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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetAllStepsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAllStepsReactor.class);

	public GetAllStepsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), "sessionId", "fileName" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String sessionId = this.keyValue.get(this.keysToGet[1]);
		String fileName = this.keyValue.get(this.keysToGet[2]);

		if (sessionId == null || sessionId.isEmpty()) {
			throw new IllegalArgumentException("sessionId is required");
		}

		if (fileName == null || fileName.isEmpty()) {
			throw new IllegalArgumentException("fileName is required");
		}

		PlaywrightSession playwrightSession = this.insight.getUser().getPlaywrightSession(sessionId);
		if (playwrightSession == null) {
			throw new IllegalStateException("No active session found for sessionId: " + sessionId);
		}

		// Load steps from file
		StepsEnvelope env = PlaywrightUtility.loadStepsFromFile(projectId, fileName);
		if (env == null) {
			throw new IllegalStateException("Failed to load steps from file: " + fileName);
		}

		populateSessionHistory(playwrightSession, env);

		// Transform the steps from Map<String, List<List<Step>>> to the frontend format
		Map<String, List<Map<String, Object>>> transformedSteps = new HashMap<>();

		for (Map.Entry<String, List<List<PlaywrightStep>>> entry : env.steps().entrySet()) {
			String tabId = entry.getKey();
			List<List<PlaywrightStep>> pages = entry.getValue();

			List<Map<String, Object>> tabSteps = new ArrayList<>();
			for (List<PlaywrightStep> page : pages) {
				for (PlaywrightStep step : page) {
					tabSteps.add(convertStepToMap(step));
				}
			}
			transformedSteps.put(tabId, tabSteps);
		}

		Map<String, Object> response = new HashMap<>();
		response.put("steps", transformedSteps);
		response.put("success", true);
		response.put("sessionId", sessionId);
		return new NounMetadata(response, PixelDataType.MAP);
	}

	/**
	 * Populates the session history with steps from the loaded recording. This
	 * appends steps from the loaded file to the existing session history, allowing
	 * multiple files to be loaded sequentially into the same session. Step IDs are
	 * reassigned to ensure uniqueness across all loaded files.
	 *
	 * @param session The PlaywrightSession to populate
	 * @param env     The StepsEnvelope containing the loaded steps
	 */
	private void populateSessionHistory(PlaywrightSession session, StepsEnvelope env) {
		Map<String, List<List<PlaywrightStep>>> stepsMap = env.steps();

		int currentStepId = session.lastStepId;

		for (Map.Entry<String, List<List<PlaywrightStep>>> entry : stepsMap.entrySet()) {
			String tabId = entry.getKey();
			List<List<PlaywrightStep>> pages = entry.getValue();

			if (!session.history.steps().containsKey(tabId)) {
				session.history.steps().put(tabId, new ArrayList<>());
			}

			for (List<PlaywrightStep> page : pages) {
				List<PlaywrightStep> pageWithNewIds = new ArrayList<>();

				for (PlaywrightStep step : page) {
					currentStepId++;
					PlaywrightStep stepWithNewId = new PlaywrightStep(step, currentStepId);
					pageWithNewIds.add(stepWithNewId);
				}

				session.history.steps().get(tabId).add(pageWithNewIds);
			}
		}

		session.lastStepId = currentStepId;
		classLogger.debug("Loaded and appended steps from file. New lastStepId: {}", currentStepId);
	}

	private Map<String, Object> convertStepToMap(PlaywrightStep step) {
		Map<String, Object> stepMap = new HashMap<>();
		stepMap.put("id", step.id());
		stepMap.put("type", step.type() != null ? step.type().toString() : null);
		stepMap.put("url", step.url());
		stepMap.put("coords", convertCoords(step.coords()));
		stepMap.put("multiCoords", convertMultiCoords(step.multiCoords()));
		stepMap.put("prompt", step.prompt());
		stepMap.put("text", step.text());
		stepMap.put("pressEnter", step.pressEnter());
		stepMap.put("deltaY", step.deltaY());
		stepMap.put("waitUntil", step.waitUntil());
		stepMap.put("waitAfterMs", step.waitAfterMs());
		stepMap.put("viewport", convertViewport(step.viewport()));
		stepMap.put("timestamp", step.timestamp());
		stepMap.put("label", step.label());
		stepMap.put("description", step.description());
		stepMap.put("isPassword", step.isPassword());
		stepMap.put("storeValue", step.storeValue());
		stepMap.put("selector", convertSelector(step.selector()));
		stepMap.put("isTriggerNewTab", convertTriggerNewTab(step.isTriggerNewTab()));
		stepMap.put("shouldRun", step.shouldRun());
		stepMap.put("required", step.required());
		stepMap.put("sendToPlayground", step.sendToPlayground());
		stepMap.put("tag", step.tag());
		return stepMap;
	}

	private Map<String, Object> convertCoords(Coords coords) {
		if (coords == null) {
			return null;
		}
		Map<String, Object> coordsMap = new HashMap<>();
		coordsMap.put("x", coords.x());
		coordsMap.put("y", coords.y());
		return coordsMap;
	}

	private List<Map<String, Object>> convertMultiCoords(List<Coords> coordsList) {
		if (coordsList == null) {
			return null;
		}
		List<Map<String, Object>> converted = new ArrayList<>(coordsList.size());
		for (Coords coord : coordsList) {
			converted.add(convertCoords(coord));
		}
		return converted;
	}

	private Map<String, Object> convertViewport(Viewport viewport) {
		if (viewport == null) {
			return null;
		}
		Map<String, Object> viewportMap = new HashMap<>();
		viewportMap.put("width", viewport.width());
		viewportMap.put("height", viewport.height());
		viewportMap.put("deviceScaleFactor", viewport.deviceScaleFactor());
		return viewportMap;
	}

	private Map<String, Object> convertSelector(Selector selector) {
		if (selector == null) {
			return null;
		}
		Map<String, Object> selectorMap = new HashMap<>();
		selectorMap.put("strategy", selector.strategy());
		selectorMap.put("value", selector.value());
		selectorMap.put("frameSelector", selector.frameSelector());
		return selectorMap;
	}

	private Map<String, Object> convertTriggerNewTab(TriggerNewTab triggerNewTab) {
		if (triggerNewTab == null) {
			return null;
		}
		Map<String, Object> triggerMap = new HashMap<>();
		triggerMap.put("isTrue", triggerNewTab.isTrue());
		triggerMap.put("tabId", triggerNewTab.tabId());
		return triggerMap;
	}

	@Override
	public String getReactorDescription() {
		return "Retrieves all recorded Playwright steps from a specified file and formats them for frontend display.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		} else if (key.equals("fileName")) {
			return "The name of the record file";
		}

		return super.getDescriptionForKey(key);
	}
}
