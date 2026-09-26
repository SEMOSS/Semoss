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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class StepReactor extends AbstractReactor {

	/**
	 * Use Gson instead of Jackson ObjectMapper to avoid Jackson version conflicts
	 * (NoSuchMethodError on ParserMinimalBase with StreamReadConstraints).
	 */
	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
	private Map<String, Object> response = new HashMap<>();

	/**
	 * Default constructor for StepReactor. Initializes the keys this reactor
	 * expects: sessionId, tabId, shouldStore, paramValues, pageIndex, and stepIndex.
	 */
	public StepReactor() {
		this.keysToGet = new String[] { "sessionId", "tabId", "shouldStore",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), "pageIndex", "stepIndex" };
		this.keyRequired = new int[] { 1, 1, 0, 1, 0, 0 };
	}

	/**
	 * Executes a single Playwright step within an active session and records it.
	 * If pageIndex and stepIndex are provided, the step is inserted at that position
	 * in the history instead of being appended.
	 *
	 * @return A NounMetadata object containing a screenshot of the page after the
	 *         step execution.
	 * @throws IllegalArgumentException If required parameters are missing or
	 *                                  invalid, or if a CONTEXT step is malformed.
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		String tabId = this.keyValue.get(this.keysToGet[1]);

		Map<String, Object> paramValues = getMap(this.keysToGet[3]);
		if (paramValues == null) {
			// Try to read as a list (the Pixel syntax [ {...} ] can land as a list item)
			GenRowStruct grs = getGenRowStruct(this.keysToGet[3]);
			if (grs != null && !grs.isEmpty()) {
				Object val = grs.get(0);
				if (val instanceof java.util.List) {
					java.util.List<?> list = (java.util.List<?>) val;
					if (!list.isEmpty() && list.get(0) instanceof Map) {
						@SuppressWarnings("unchecked")
						Map<String, Object> m = (Map<String, Object>) list.get(0);
						paramValues = m;
					}
				}
			}
		}
		if (paramValues == null) {
			throw new IllegalArgumentException(
					"Step reactor requires 'paramValues' to be a non-null map of step parameters.");
		}
		// Convert Map → JSON string → PlaywrightStep using Gson (avoids Jackson
		// version conflicts with StreamReadConstraints in Jackson 2.15+)
		PlaywrightStep step = GSON.fromJson(GSON.toJson(paramValues), PlaywrightStep.class);
		if (step == null || step.type() == null) {
			throw new IllegalArgumentException(
					"Could not deserialize paramValues into a PlaywrightStep. Ensure 'type' is set to a valid value: "
							+ "NAVIGATE, CLICK, TYPE, SCROLL, WAIT, HOVER, CONTEXT.");
		}

		ScreenshotResponse screenshotResponse = step.type() == PlaywrightStepType.CONTEXT
				? executeContextStep(sessionId, step, tabId)
				: executeStep(sessionId, step, tabId);

		response.put("screenshot", screenshotResponse);

		return new NounMetadata(response, PixelDataType.MAP);
	}

	/**
	 * Executes a CONTEXT type step. This involves adding the step to the session
	 * history and capturing a screenshot.
	 *
	 * @param sessionId The ID of the current Playwright session.
	 * @param step      The {@link PlaywrightStep} of type CONTEXT to execute.
	 * @param tabId     The ID of the tab where the step is executed.
	 * @return A {@link ScreenshotResponse} captured after adding the context step.
	 * @throws IllegalArgumentException If the CONTEXT step is missing required
	 *                                  multiCoords or prompt.
	 */
	public ScreenshotResponse executeContextStep(String sessionId, PlaywrightStep step, String tabId) {
		PlaywrightSession playwrightSession = this.insight.getUser().getPlaywrightSession(sessionId);
		if (playwrightSession == null) {
			throw new IllegalArgumentException(
					"Playwright session '" + sessionId + "' not found. Please start a new session.");
		}

		int stepId = ++playwrightSession.lastStepId;
		PlaywrightStep newStep = new PlaywrightStep(step, stepId);

		if (newStep.multiCoords().isEmpty() || newStep.prompt().isEmpty()) {
			throw new IllegalArgumentException("CONTEXT step requires multiCoords and prompt to be non-empty.");
		}

		if (playwrightSession.history.steps().isEmpty() || playwrightSession.history.steps().size() <= 1) {
			playwrightSession.history.steps().get(tabId).add(new ArrayList<>(List.of(newStep)));
		} else {
			playwrightSession.history.steps().get(tabId).getLast().add(newStep);
		}

		response.put("stepId", stepId);
		return ScreenshotReactor.screenshot(playwrightSession, tabId);
	}

	/**
	 * Executes a non-CONTEXT Playwright step and records it into the session
	 * history.
	 *
	 * @param sessionId The ID of the current Playwright session.
	 * @param step      The {@link PlaywrightStep} to execute.
	 * @param tabId     The ID of the tab where the step is executed.
	 * @return A {@link ScreenshotResponse} captured after the step execution,
	 *         potentially from a new tab.
	 */
	public ScreenshotResponse executeStep(String sessionId, PlaywrightStep step, String tabId) {
		PlaywrightSession playwrightSession = this.insight.getUser().getPlaywrightSession(sessionId);
		if (playwrightSession == null) {
			throw new IllegalArgumentException(
					"Playwright session '" + sessionId + "' not found. The session may have expired or the server was restarted. Please start a new session.");
		}
		Map<String, Object> stepResult = PlaywrightSessionUtility.applyStep(playwrightSession, step, tabId);
		boolean isPageChanged = (Boolean) stepResult.get("isPageChanged");
		boolean isNewTab = (Boolean) stepResult.get("isNewTab");

		String newTabId = null;

		// If a new tab was opened, capture the new tab ID
		if (isNewTab) {
			newTabId = (String) stepResult.get("newTabId");
		}

		addStepToHistory(playwrightSession, step, isPageChanged, tabId, isNewTab, newTabId);
		response.put("isNewTab", isNewTab);

		if (newTabId != null) {
			response.put("newTabId", newTabId);
		}

		if (stepResult.get("tabTitle") != null) {
			response.put("tabTitle", stepResult.get("tabTitle"));
		}

		// Return screenshot from the NEW tab if one was opened, otherwise from the
		// current tab
		String screenshotTabId = isNewTab && newTabId != null ? newTabId : tabId;
		return ScreenshotReactor.screenshot(this.insight.getUser().getPlaywrightSession(sessionId), screenshotTabId);
	}

	/**
	 * Adds a {@link PlaywrightStep} to the session's history, handling new tab
	 * creation and page change logic. If pageIndex and stepIndex are provided,
	 * inserts the step at that specific position instead of appending.
	 *
	 * @param playwrightSession The active {@link PlaywrightSession}.
	 * @param step              The {@link PlaywrightStep} to add.
	 * @param isPageChanged     True if the step resulted in a page change.
	 * @param tabId             The ID of the tab where the step occurred.
	 * @param isNewTab          True if the step triggered a new tab.
	 * @param newTabId          The ID of the new tab, if created.
	 */
	private void addStepToHistory(PlaywrightSession playwrightSession, PlaywrightStep step, boolean isPageChanged,
			String tabId, boolean isNewTab, String newTabId) {
		String shouldStoreParam = this.keyValue.get(this.keysToGet[2]);
		boolean shouldStore = Boolean.parseBoolean(shouldStoreParam);
		int stepId = ++playwrightSession.lastStepId;
		PlaywrightStep newStep = new PlaywrightStep(step, stepId);

		if (!shouldStore && step.type() == PlaywrightStepType.TYPE) {
			newStep = new PlaywrightStep(stepId, step.type(), step.url(), step.coords(), step.multiCoords(),
				step.prompt(), "", step.pressEnter(), step.deltaY(), step.waitUntil(), step.waitAfterMs(),
				step.viewport(), step.timestamp(), step.label(), step.description(), step.isPassword(),
				step.storeValue(), step.selector(), step.isTriggerNewTab(), step.shouldRun(), step.required(),
				step.sendToPlayground(), step.tag(), step.downloadExpected());
		}

		if (isNewTab && newTabId != null) {
			TriggerNewTab triggerNewTab = new TriggerNewTab(true, newTabId);
			newStep = new PlaywrightStep(stepId, newStep.type(), newStep.url(), newStep.coords(), step.multiCoords(),
					step.prompt(), newStep.text(), newStep.pressEnter(), newStep.deltaY(), newStep.waitUntil(),
					newStep.waitAfterMs(), newStep.viewport(), newStep.timestamp(), newStep.label(),
					newStep.description(), newStep.isPassword(), newStep.storeValue(), newStep.selector(),
					triggerNewTab, newStep.shouldRun(), newStep.required(), step.sendToPlayground(), step.tag(),
					step.downloadExpected());
			playwrightSession.addChildTabRelationship(tabId, newTabId);
		}

		String stepIndexStr = this.keyValue.get(this.keysToGet[5]);

		if (stepIndexStr != null) {
			insertStepAtPosition(playwrightSession, tabId, newStep, stepIndexStr);
		} else {
			if (isPageChanged) {
				playwrightSession.history.steps().get(tabId).add(new ArrayList<>(List.of(newStep)));
			} else {
				if (playwrightSession.history.steps().isEmpty() || playwrightSession.history.steps().size() <= 1) {
					playwrightSession.history.steps().get(tabId).add(new ArrayList<>(List.of(newStep)));
				} else {
					playwrightSession.history.steps().get(tabId).getLast().add(newStep);
				}
			}
		}

		response.put("stepId", stepId);
	}

	/**
	 * Inserts a step at a specific position in the history.
	 * If stepIndex is out of bounds, appends as a new page at the end.
	 *
	 * @param playwrightSession The active {@link PlaywrightSession}.
	 * @param tabId             The ID of the tab.
	 * @param newStep           The step to insert.
	 * @param stepIndexStr      The step index as a string.
	 */
	private void insertStepAtPosition(PlaywrightSession playwrightSession, String tabId, PlaywrightStep newStep,
			String stepIndexStr) {
		int stepIndex = Integer.parseInt(stepIndexStr);

		List<List<PlaywrightStep>> tabHistory = playwrightSession.history.steps().get(tabId);

		// If stepIndex is out of bounds, append to the end of history as a new page
		if (stepIndex < 0 || stepIndex >= tabHistory.size()) {
			tabHistory.add(new ArrayList<>(List.of(newStep)));
			response.put("insertedAtStep", tabHistory.size() - 1);
			response.put("appendedDueToOutOfBounds", true);
			return;
		}

		tabHistory.add(stepIndex, new ArrayList<>(List.of(newStep)));

		response.put("insertedAtStep", stepIndex);
	}

	@Override
	public String getReactorDescription() {
		return "Execute a step in the current page of the playwright session";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "Playwright session ID that stores information about the history of actions done during that session";
		} else if (key.equals("shouldStore")) {
			return "Boolean flag to indicate whether to store the value of TYPE actions in the session history. If false, the value will be replaced with an empty string.";
		} else if (key.equals("paramValues")) {
			return "Map of step parameters. Required keys: type (NAVIGATE, CLICK, TYPE, SCROLL, WAIT), url (for NAVIGATE), coords (for CLICK and TYPE), text (for TYPE), pressEnter (for TYPE), deltaY (for SCROLL), waitAfterMs (optional for all types)";
		} else if (key.equals("pageIndex")) {
			return "Optional. Zero-based index of the page within the tab where the step should be inserted after execution. If not provided, step is appended based on page change detection.";
		} else if (key.equals("stepIndex")) {
			return "Optional. Zero-based index position where the step should be inserted within the page after execution. Required if pageIndex is provided. Use 0 to insert at the beginning, or any value up to the current number of steps to append.";
		}

		return super.getDescriptionForKey(key);
	}
}
