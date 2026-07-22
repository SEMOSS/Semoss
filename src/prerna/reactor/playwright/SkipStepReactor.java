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

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SkipStepReactor extends AbstractReactor {

	private ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	private Path recordingsDir = null;
	private StepsEnvelope stepsEnvelope;
	private Map<String, Object> response = new HashMap<>();

	/**
	 * Default constructor for SkipStepReactor. Initializes the keys this reactor
	 * expects: sessionId, fileName, tabId, and projectId.
	 */
	public SkipStepReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), "sessionId", "fileName", "tabId" };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	/**
	 * Executes the reactor to skip the current step in the Playwright session.
	 *
	 * @return A NounMetadata object containing the updated session state, including
	 *         whether it's the last page and the next set of actions.
	 * @throws IllegalArgumentException If sessionId or fileName is missing or
	 *                                  empty.
	 * @throws IllegalStateException    If the Playwright session is not found.
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String sessionId = this.keyValue.get(this.keysToGet[1]);
		String fileName = this.keyValue.get(this.keysToGet[2]);
		String tabId = this.keyValue.get(this.keysToGet[3]);
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to view the project");
		}

		if (sessionId == null || sessionId.isEmpty()) {
			throw new IllegalArgumentException("sessionId is required");
		}
		if (fileName == null || fileName.isEmpty()) {
			throw new IllegalArgumentException("fileName is required");
		}
		PlaywrightSession playwrightSession = this.insight.getUser().getPlaywrightSession(sessionId);
		if (playwrightSession == null) {
			throw new IllegalStateException("Session not found: " + sessionId);
		}

		recordingsDir = PlaywrightUtility.initRecordingsDir(projectId);
		// Load steps from file
		stepsEnvelope = loadStepsFromFile(fileName);
		List<List<PlaywrightStep>> allStepsList = stepsEnvelope.steps().entrySet().iterator().next().getValue();

		// Skip the current step
		skipStep(playwrightSession, allStepsList, tabId);

		// Return updated session state
		return new NounMetadata(response, PixelDataType.MAP);
	}

	/**
	 * Skips the current step in the Playwright session, advancing the step and page
	 * indices.
	 *
	 * @param session      The active {@link PlaywrightSession}.
	 * @param allStepsList A list of all pages, each containing a list of
	 *                     {@link PlaywrightStep}s for the current tab.
	 * @param tabId        The ID of the tab to skip the step for.
	 */
	private void skipStep(PlaywrightSession session, List<List<PlaywrightStep>> allStepsList, String tabId) {
		// Validate inputs
		if (allStepsList == null || allStepsList.isEmpty()) {
			session.isLastPage = true;
			response.put("isLastPage", true);
			return;
		}

		// Validate current page index
		if (session.getCurrentPageIndex(tabId) < 0 || session.getCurrentPageIndex(tabId) >= allStepsList.size()) {
			session.isLastPage = true;
			response.put("isLastPage", true);
			return;
		}

		session.incrementStepIndex(tabId);

		// Check if the current step index exceeds the steps in the current page
		List<PlaywrightStep> currentPageSteps = allStepsList.get(session.getCurrentPageIndex(tabId));
		if (currentPageSteps != null && session.getCurrentStepIndex(tabId) >= currentPageSteps.size()) {
			// Move to the next page if there are more pages
			if (session.getCurrentPageIndex(tabId) < allStepsList.size() - 1) {
				session.incrementPageIndex(tabId);
				session.setCurrentStepIndex(tabId, 0);// Reset step index for the new page
			} else {
				// If no more pages, set the session to the last page
				session.isLastPage = true;
			}
		}
		response.put("isLastPage", session.isLastPage);
		if (session.getCurrentPageIndex(tabId) < allStepsList.size()) {
			response.put("actions", getPageActions(allStepsList.get(session.getCurrentPageIndex(tabId)),
					session.getCurrentStepIndex(tabId)));
		}
	}

	/**
	 * Loads a {@link StepsEnvelope} from a JSON file.
	 *
	 * @param nameOrPath The name or full path of the recording file.
	 * @return The loaded {@link StepsEnvelope}.
	 * @throws RuntimeException If the file cannot be read or parsed.
	 */
	private StepsEnvelope loadStepsFromFile(String nameOrPath) {
		Path file = nameOrPath.contains(FileSystems.getDefault().getSeparator()) ? Paths.get(nameOrPath)
				: recordingsDir.resolve(nameOrPath.endsWith(".json") ? nameOrPath : nameOrPath + ".json");

		try {
			return json.readValue(file.toFile(), StepsEnvelope.class);
		} catch (Exception e) {
			throw new RuntimeException("Failed to read: " + file, e);
		}
	}

	/**
	 * Formats a list of {@link PlaywrightStep}s into a list of maps suitable for
	 * frontend display as actions.
	 *
	 * @param steps            The list of {@link PlaywrightStep}s to format.
	 * @param currentStepIndex The index of the current step to start formatting
	 *                         from.
	 * @return A list of maps, each representing a pending action.
	 */
	private static List<Map<String, Object>> getPageActions(List<PlaywrightStep> steps, int currentStepIndex) {
		List<Map<String, Object>> actionsList = new ArrayList<>();
		if (steps == null || steps.isEmpty()) {
			return actionsList;
		}
		if (currentStepIndex < 0 || currentStepIndex >= steps.size()) {
			return actionsList; // or throw exception
		}
		for (int i = currentStepIndex; i < steps.size(); i++) {
			Map<String, Object> action = new HashMap<>();
			PlaywrightStep current = steps.get(i);
			switch (current.type()) {
			case TYPE:
				Map<String, Object> typeAction = new HashMap<>();
				typeAction.put("label", current.label());
				typeAction.put("text", current.text());
				typeAction.put("isPassword", current.isPassword());
				typeAction.put("coords", current.coords());
				typeAction.put("storeValue", current.storeValue());
				typeAction.put("selector", current.selector());
				action.put("TYPE", typeAction);
				break;
			case CLICK:
				action.put("CLICK", current.coords());
				break;
			case SCROLL:
				action.put("SCROLL", current.deltaY());
				break;
			case NAVIGATE:
				action.put("NAVIGATE", current.url());
				break;
			case WAIT:
				action.put("WAIT", current.waitAfterMs());
				break;
			case CONTEXT:
				action.put("CONTEXT", Map.of(current.multiCoords(), current.prompt()));
				break;
			case HOVER:
				action.put("HOVER", current.coords());
				break;
			default:
				break;
			}
			actionsList.add(action);
		}
		return actionsList;
	}

	@Override
	public String getReactorDescription() {
		return "Skips the current step in the Playwright session, advancing the step and page indices.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "Playwright session ID that stores information about the history of actions done during that session";
		} else if (key.equals("fileName")) {
			return "File name containing the steps to be replayed";
		}

		return super.getDescriptionForKey(key);
	}

}
