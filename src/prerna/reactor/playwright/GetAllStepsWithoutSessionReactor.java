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

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Loads all Playwright steps from a saved recording file without requiring an
 * active browser session.
 */
public class GetAllStepsWithoutSessionReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetAllStepsWithoutSessionReactor.class);

	public GetAllStepsWithoutSessionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), "fileName" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String fileName = this.keyValue.get(this.keysToGet[1]);
		User user = this.insight.getUser();

		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("project is required");
		}

		if (!SecurityProjectUtils.userCanViewProject(user, projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		if (fileName == null || fileName.isEmpty()) {
			throw new IllegalArgumentException("fileName is required");
		}

		StepsEnvelope env = PlaywrightUtility.loadStepsFromFile(projectId, fileName);
		if (env == null) {
			throw new IllegalStateException("Failed to load steps from file: " + fileName);
		}

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
		return new NounMetadata(response, PixelDataType.MAP);
	}

	/**
	 * Converts a {@link PlaywrightStep} into a serializable map.
	 *
	 * @param step step to convert
	 * @return converted step map
	 */
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

	/**
	 * Converts coordinates into a serializable map.
	 *
	 * @param coords coordinate object
	 * @return coordinate map or {@code null} when coords are not present
	 */
	private Map<String, Object> convertCoords(Coords coords) {
		if (coords == null) {
			return null;
		}
		Map<String, Object> coordsMap = new HashMap<>();
		coordsMap.put("x", coords.x());
		coordsMap.put("y", coords.y());
		return coordsMap;
	}

	/**
	 * Converts a list of coordinates into a serializable list.
	 *
	 * @param coordsList list of coordinates
	 * @return converted coordinate list or {@code null} when not present
	 */
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

	/**
	 * Converts viewport information into a serializable map.
	 *
	 * @param viewport viewport configuration
	 * @return viewport map or {@code null} when viewport is not present
	 */
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

	/**
	 * Converts selector information into a serializable map.
	 *
	 * @param selector selector details
	 * @return selector map or {@code null} when selector is not present
	 */
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

	/**
	 * Converts trigger-new-tab metadata into a serializable map.
	 *
	 * @param triggerNewTab trigger metadata
	 * @return trigger map or {@code null} when metadata is not present
	 */
	private Map<String, Object> convertTriggerNewTab(TriggerNewTab triggerNewTab) {
		if (triggerNewTab == null) {
			return null;
		}
		Map<String, Object> triggerMap = new HashMap<>();
		triggerMap.put("isTrue", triggerNewTab.isTrue());
		triggerMap.put("tabId", triggerNewTab.tabId());
		return triggerMap;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getReactorDescription() {
		return "Loads all recorded Playwright steps from a saved file without requiring an active Playwright session.";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.PROJECT.getKey().equals(key)) {
			return "Project where the recording file exists.";
		} else if ("fileName".equals(key)) {
			return "Recording file name to load, relative to the project's app asset folder.";
		}
		return super.getDescriptionForKey(key);
	}
}
