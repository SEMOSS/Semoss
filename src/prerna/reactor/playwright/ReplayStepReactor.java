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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.options.LoadState;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ReplayStepReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ReplayStepReactor.class);

	private ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
	private Map<String, Object> response = new HashMap<>();
	private Path recordingsDir = null;
	private String projectId = null;

	/**
	 * Default constructor for ReplayStepReactor. Initializes the keys this reactor
	 * expects: sessionId, fileName, paramValues, executeAll, tabId, and projectId.
	 */
	public ReplayStepReactor() {
		this.keysToGet = new String[] { "sessionId", "fileName", ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				"executeAll", "tabId", ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 1 };
	}

	/**
	 * Executes the reactor to replay Playwright steps from a recorded script.
	 *
	 * @return A NounMetadata object containing the result of the replay, including
	 *         a screenshot, next actions, and session state information.
	 * @throws IllegalArgumentException If required parameters are missing or
	 *                                  invalid.
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String name = this.keyValue.get(this.keysToGet[1]);
		Map<String, Object> inputs = getMap(this.keysToGet[2]);
		String tabId = this.keyValue.get(this.keysToGet[4]);

		projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must input a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityProjectUtils.userCanViewProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to view the project");
		}
		recordingsDir = PlaywrightUtility.initRecordingsDir(projectId);

		ScreenshotResponse screenshot = replayFromFile(inputs, name, tabId);
		response.put("screenshot", screenshot);

		return new NounMetadata(response, PixelDataType.MAP);
	}

	/**
	 * Replays steps from a specified recording file.
	 *
	 * @param inputs     A map of input values for TYPE steps.
	 * @param nameOrPath The name or path of the recording file.
	 * @param tabId      The ID of the tab to replay the steps on.
	 * @return A {@link ScreenshotResponse} captured after the replay.
	 */
	public ScreenshotResponse replayFromFile(Map<String, Object> inputs, String nameOrPath, String tabId) {
		StepsEnvelope env = PlaywrightUtility.loadStepsFromFile(projectId, nameOrPath);
		return replay(env, inputs, tabId);
	}

	/**
	 * Replays a sequence of Playwright steps from a {@link StepsEnvelope}.
	 *
	 * @param steps  The {@link StepsEnvelope} containing the steps to replay.
	 * @param inputs A map of input values for TYPE steps.
	 * @param tabId  The ID of the tab to replay the steps on.
	 * @return A {@link ScreenshotResponse} captured after the replay.
	 */
	public ScreenshotResponse replay(StepsEnvelope steps, Map<String, Object> inputs, String tabId) {
		boolean executeAll = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[3]));

		Map<String, List<List<PlaywrightStep>>> allStepsMap = steps.steps();
		String requestedTabId = (tabId != null && !tabId.isEmpty()) ? tabId : "tab-1";
		List<List<PlaywrightStep>> allStepsList = allStepsMap.getOrDefault(requestedTabId, new ArrayList<>());

		classLogger.info("Loaded steps: {}", json.valueToTree(steps).toString());

		// Determine viewport/dpr from the first step if available
		int width = 1280;
		int height = 800;
		double dpr = 1.0;
		if (!allStepsList.isEmpty() && !allStepsList.get(0).isEmpty()
				&& allStepsList.get(0).get(0).viewport() != null) {
			width = allStepsList.get(0).get(0).viewport().width();
			height = allStepsList.get(0).get(0).viewport().height();
			dpr = allStepsList.get(0).get(0).viewport().deviceScaleFactor();
		}

		// Reuse global Browser and per-user shared BrowserContext
		Browser browser = PlaywrightBrowserProvider.getBrowser();
		Browser.NewContextOptions ctxOps = new Browser.NewContextOptions().setViewportSize(width, height)
				.setDeviceScaleFactor(dpr);

		// Thread-safe get-or-create on the user object
		BrowserContext ctx = this.insight.getUser().getOrCreateSharedPlaywrightContext(browser, ctxOps);

		// Retrieve or create the Session for this request
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		PlaywrightSession playwrightSession = (sessionId != null)
				? this.insight.getUser().getPlaywrightSession(sessionId)
				: null;

		if (playwrightSession == null) {
			// Create a new page within the shared context and a new Session
			Page page = ctx.newPage();
			// Align page viewport to steps if needed (context viewport is fixed, page can
			// adjust)
			try {
				page.setViewportSize(width, height);
			} catch (Exception e) {
				classLogger.warn("Failed to set page viewport to {}x{}: {}", width, height, e.getMessage());
			}
			playwrightSession = new PlaywrightSession(ctx, page);

			if (playwrightSession.history.meta() == null) {
				playwrightSession.history = new StepsEnvelope("1.0", PlaywrightSession.newMeta(""),
						playwrightSession.history.steps());
			}
			// Use provided sessionId if present; otherwise generate one
			String newId = (sessionId != null && !sessionId.isEmpty()) ? sessionId
					: java.util.UUID.randomUUID().toString();
			playwrightSession.setUserAndSessionId(this.insight.getUser(), newId);
			this.insight.getUser().setPlaywrightSession(newId, playwrightSession);
			sessionId = newId;
			classLogger.info("Created new Session in shared context with id: {}", sessionId);
		} else {
			// Optional: update viewport on existing page to match steps
			try {
				playwrightSession.getPage().setViewportSize(width, height);
			} catch (Exception e) {
				classLogger.debug("Viewport update on existing page skipped/failed: {}", e.getMessage());
			}
		}
		ExecutionResult execResult = executeSteps(playwrightSession, allStepsMap, requestedTabId, executeAll, inputs);

		String responseTabId = execResult.newTabId != null ? execResult.newTabId : requestedTabId;

		if (execResult.newTabId != null) {
			response.put("isNewTab", true);
			response.put("newTabId", execResult.newTabId);
			response.put("tabTitle", execResult.newTabTitle);
			response.put("originalTabId", requestedTabId);
			response.put("originalTabActions", execResult.originalTabActions);
			classLogger.info("New tab opened: {}, original tab has {} remaining actions", execResult.newTabId,
					execResult.originalTabActions.size());
		}

		// Get next actions for the response tab
		List<Map<String, Object>> nextActions = getNextActions(playwrightSession, allStepsMap, responseTabId);
		response.put("actions", nextActions);

		// Calculate isLastPage
		boolean isLastPage = calculateIsLastPage(playwrightSession, allStepsMap, responseTabId);
		response.put("isLastPage", isLastPage);
		playwrightSession.isLastPage = isLastPage;

		classLogger.info("Returning {} actions for tab: {}", nextActions.size(), responseTabId);

		return ScreenshotReactor.screenshot(playwrightSession, responseTabId);
	}

	/**
	 * Executes a sequence of Playwright steps for a given tab.
	 *
	 * @param playwrightSession The active {@link PlaywrightSession}.
	 * @param allStepsMap       A map of all steps, organized by tab ID.
	 * @param tabId             The ID of the tab to execute steps on.
	 * @param executeAll        A boolean indicating whether to execute all
	 *                          remaining steps or just one.
	 * @param inputs            A map of input values for TYPE steps.
	 * @return An {@link ExecutionResult} containing information about the
	 *         execution, including new tab details.
	 */
	private ExecutionResult executeSteps(PlaywrightSession playwrightSession,
			Map<String, List<List<PlaywrightStep>>> allStepsMap, String tabId, boolean executeAll,
			Map<String, Object> inputs) {

		ExecutionResult result = new ExecutionResult();
		List<List<PlaywrightStep>> tabSteps = allStepsMap.get(tabId);

		if (tabSteps == null || tabSteps.isEmpty()) {
			return result;
		}

		if (playwrightSession.getCurrentPageIndex(tabId) == 0 && playwrightSession.getCurrentStepIndex(tabId) == 0) {
			PlaywrightStep navigateStep = tabSteps.get(0).get(0);
			Map<String, Object> stepResult = PlaywrightSessionUtility.applyStep(playwrightSession, navigateStep, tabId);
			result.newTabTitle = (String) stepResult.get("tabTitle");
			result.newTabId = tabId;
			playwrightSession.incrementPageIndex(tabId);
			classLogger.info("Executed initial NAVIGATE step for tab: {}", tabId);
			return result;
		}

		if (executeAll) {
			return executeAllSteps(playwrightSession, allStepsMap, tabId, inputs);
		} else {
			return executeSingleStep(playwrightSession, allStepsMap, tabId, inputs);
		}
	}

	/**
	 * Executes a single Playwright step for a given tab.
	 *
	 * @param playwrightSession The active {@link PlaywrightSession}.
	 * @param allStepsMap       A map of all steps, organized by tab ID.
	 * @param tabId             The ID of the tab to execute the step on.
	 * @param inputs            A map of input values for TYPE steps.
	 * @return An {@link ExecutionResult} containing information about the
	 *         execution, including new tab details.
	 */
	private ExecutionResult executeSingleStep(PlaywrightSession playwrightSession,
			Map<String, List<List<PlaywrightStep>>> allStepsMap, String tabId, Map<String, Object> inputs) {
		ExecutionResult result = new ExecutionResult();
		List<List<PlaywrightStep>> tabSteps = allStepsMap.get(tabId);

		int pageIdx = playwrightSession.getCurrentPageIndex(tabId);
		int stepIdx = playwrightSession.getCurrentStepIndex(tabId);

		if (pageIdx >= tabSteps.size()) {
			classLogger.warn("PageIndex out of bounds for tab {}", tabId);
			return result;
		}

		List<PlaywrightStep> currentPage = tabSteps.get(pageIdx);
		if (stepIdx >= currentPage.size()) {
			classLogger.warn("StepIndex out of bounds for tab {}", tabId);
			return result;
		}

		PlaywrightStep step = currentPage.get(stepIdx);
		classLogger.info("Executing step: {}", json.valueToTree(step).toString());

		// Check if step should be executed
		if (!step.shouldRun()) {
			classLogger.info("Skipping step (shouldRun=false): {}", step.id());
			playwrightSession.incrementStepIndex(tabId);

			// Move to next page if needed
			if (playwrightSession.getCurrentStepIndex(tabId) >= currentPage.size()) {
				if (pageIdx < tabSteps.size() - 1) {
					playwrightSession.incrementPageIndex(tabId);
					playwrightSession.setCurrentStepIndex(tabId, 0);
					classLogger.info("Moving to next page for tab {}", tabId);
				}
			}
			return result;
		}

		// Apply the step
		if (step.type() == PlaywrightStepType.TYPE && inputs != null && inputs.containsKey(step.label())) {
			PlaywrightStep newStep = new PlaywrightStep(step, inputs.get(step.label()).toString());
			PlaywrightSessionUtility.applyStep(playwrightSession, newStep, tabId);
		} else {
			PlaywrightSessionUtility.applyStep(playwrightSession, step, tabId);
		}

		// Increment step index
		playwrightSession.incrementStepIndex(tabId);

		// Check if we need to move to next page
		if (playwrightSession.getCurrentStepIndex(tabId) >= currentPage.size()) {
			if (pageIdx < tabSteps.size() - 1) {
				playwrightSession.incrementPageIndex(tabId);
				playwrightSession.setCurrentStepIndex(tabId, 0);
				classLogger.info("Moving to next page for tab {}", tabId);
			}
		}

		// Handle new tab trigger
		if (step.isTriggerNewTab() != null && step.isTriggerNewTab().isTrue()) {
			String newTabId = step.isTriggerNewTab().tabId();
			result.newTabId = newTabId;

			// Initialize new tab indices
			if (!playwrightSession.tabCurrentPageIndex.containsKey(newTabId)) {
				playwrightSession.setCurrentPageIndex(newTabId, 0);
				playwrightSession.setCurrentStepIndex(newTabId, 0);
			}

			// Get title
			Page newTabPage = playwrightSession.getPage(newTabId);
			result.newTabTitle = (newTabPage != null && newTabPage.title() != null
					&& !newTabPage.title().trim().isEmpty()) ? newTabPage.title() : newTabId;

			// Capture remaining actions for original tab
			result.originalTabActions = getNextActions(playwrightSession, allStepsMap, tabId);

			classLogger.info("Step triggered new tab: {}", newTabId);
		}

		return result;
	}

	/**
	 * Executes all remaining Playwright steps for a given tab.
	 *
	 * @param playwrightSession The active {@link PlaywrightSession}.
	 * @param allStepsMap       A map of all steps, organized by tab ID.
	 * @param tabId             The ID of the tab to execute steps on.
	 * @param inputs            A map of input values for TYPE steps.
	 * @return An {@link ExecutionResult} containing information about the
	 *         execution, including new tab details.
	 */
	private ExecutionResult executeAllSteps(PlaywrightSession playwrightSession,
			Map<String, List<List<PlaywrightStep>>> allStepsMap, String tabId, Map<String, Object> inputs) {
		ExecutionResult result = new ExecutionResult();
		List<List<PlaywrightStep>> tabSteps = allStepsMap.get(tabId);

		int pageIdx = playwrightSession.getCurrentPageIndex(tabId);
		if (pageIdx >= tabSteps.size()) {
			return result;
		}

		List<PlaywrightStep> currentPage = tabSteps.get(pageIdx);

		// Execute all steps on current page
		while (playwrightSession.getCurrentStepIndex(tabId) < currentPage.size()) {
			PlaywrightStep step = currentPage.get(playwrightSession.getCurrentStepIndex(tabId));

			// Check if step should be executed
			if (!step.shouldRun()) {
				classLogger.info("Skipping step (shouldRun=false): {}", step.id());
				playwrightSession.incrementStepIndex(tabId);
				continue;
			}

			if (step.type() == PlaywrightStepType.TYPE && inputs != null && inputs.containsKey(step.label())) {
				PlaywrightStep newStep = new PlaywrightStep(step, inputs.get(step.label()).toString());
				PlaywrightSessionUtility.applyStep(playwrightSession, newStep, tabId);
			} else {
				PlaywrightSessionUtility.applyStep(playwrightSession, step, tabId);
			}

			playwrightSession.incrementStepIndex(tabId);

			// Handle new tab
			if (step.isTriggerNewTab() != null && step.isTriggerNewTab().isTrue()) {
				String newTabId = step.isTriggerNewTab().tabId();
				if (!playwrightSession.tabCurrentPageIndex.containsKey(newTabId)) {
					playwrightSession.setCurrentPageIndex(newTabId, 0);
					playwrightSession.setCurrentStepIndex(newTabId, 0);
				}
				result.newTabId = newTabId;
				classLogger.info("Step triggered new tab during executeAll: {}", newTabId);
			}
		}

		// Move to next page if not last
		if (pageIdx < tabSteps.size() - 1) {
			playwrightSession.incrementPageIndex(tabId);
			playwrightSession.setCurrentStepIndex(tabId, 0);
		}

		return result;
	}

	/**
	 * Retrieves a list of the next actions (steps) to be executed for a given tab.
	 *
	 * @param playwrightSession The active {@link PlaywrightSession}.
	 * @param allStepsMap       A map of all steps, organized by tab ID.
	 * @param tabId             The ID of the tab to get actions for.
	 * @return A list of maps, where each map represents a pending action.
	 */
	private List<Map<String, Object>> getNextActions(PlaywrightSession playwrightSession,
			Map<String, List<List<PlaywrightStep>>> allStepsMap, String tabId) {
		List<List<PlaywrightStep>> tabSteps = allStepsMap.get(tabId);

		if (tabSteps == null || tabSteps.isEmpty()) {
			return new ArrayList<>();
		}

		int pageIdx = playwrightSession.getCurrentPageIndex(tabId);
		int stepIdx = playwrightSession.getCurrentStepIndex(tabId);

		if (pageIdx >= tabSteps.size()) {
			classLogger.info("No more pages for tab {}", tabId);
			return new ArrayList<>();
		}

		List<PlaywrightStep> currentPage = tabSteps.get(pageIdx);
		return getPageActions(currentPage, stepIdx, tabId);
	}

	/**
	 * Calculates whether the current tab has reached the last page and completed
	 * all its steps.
	 *
	 * @param playwrightSession The active {@link PlaywrightSession}.
	 * @param allStepsMap       A map of all steps, organized by tab ID.
	 * @param tabId             The ID of the tab to check.
	 * @return True if all steps for the current tab are completed, false otherwise.
	 */
	private boolean calculateIsLastPage(PlaywrightSession playwrightSession,
			Map<String, List<List<PlaywrightStep>>> allStepsMap, String tabId) {
		List<List<PlaywrightStep>> tabSteps = allStepsMap.get(tabId);

		if (tabSteps == null || tabSteps.isEmpty()) {
			return true;
		}

		int pageIdx = playwrightSession.getCurrentPageIndex(tabId);
		int stepIdx = playwrightSession.getCurrentStepIndex(tabId);

		if (pageIdx >= tabSteps.size()) {
			return true;
		}

		boolean isLastPage = pageIdx == tabSteps.size() - 1;
		boolean completedAllSteps = stepIdx >= tabSteps.get(pageIdx).size();

		return isLastPage && completedAllSteps;
	}

	/**
	 * Lists all Playwright recording files (JSON) in the recordings directory.
	 *
	 * @return A list of filenames of the recordings.
	 * @throws RuntimeException If there is an error listing the recordings.
	 */
	public List<String> listRecordings() {
		try (var stream = Files.list(recordingsDir)) {
			return stream.filter(p -> p.getFileName().toString().endsWith(".json")).map(p -> p.getFileName().toString())
					.sorted().toList();
		} catch (Exception e) {
			throw new RuntimeException("Failed to list recordings", e);
		}
	}

	/**
	 * Formats a list of {@link PlaywrightStep}s into a list of maps suitable for
	 * frontend display as actions.
	 *
	 * @param steps            The list of {@link PlaywrightStep}s to format.
	 * @param currentStepIndex The index of the current step to start formatting
	 *                         from.
	 * @param tabId            The ID of the tab associated with these steps.
	 * @return A list of maps, each representing an action with its details.
	 */
	private List<Map<String, Object>> getPageActions(List<PlaywrightStep> steps, int currentStepIndex, String tabId) {
		List<Map<String, Object>> actionsList = new ArrayList<>();
		for (int i = currentStepIndex; i < steps.size(); i++) {
			Map<String, Object> action = new HashMap<>();
			PlaywrightStep current = steps.get(i);
			classLogger.info("Processing step for actions: {}", json.valueToTree(current).toString());
			classLogger.info("coords: {}", current.coords());
			switch (current.type()) {
			case TYPE:
				Map<String, Object> typeAction = new HashMap<>();
				typeAction.put("label", current.label());
				typeAction.put("description", current.description());
				typeAction.put("text", current.text());
				typeAction.put("isPassword", current.isPassword());
				typeAction.put("coords", current.coords());

				try {
					String sessionId = this.keyValue.get(this.keysToGet[0]);
					PlaywrightSession s = this.insight.getUser().getPlaywrightSession(sessionId);
					Page page = s.getPage(tabId);
					page.waitForLoadState(LoadState.NETWORKIDLE, new Page.WaitForLoadStateOptions().setTimeout(5_000));
					ElementProbeResponse probeResult = ProbeElementReactor.probeElementAt(s, current.coords(), tabId);
					typeAction.put("probe", probeResult);
				} catch (Exception e) {
					throw new RuntimeException(e.getMessage());
				}
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
			case CONTEXT:
				action.put("CONTEXT", Map.of("multiCoords", current.multiCoords(), "prompt", current.prompt()));
				break;
			case HOVER:
				action.put("HOVER", current.coords());
				break;
			default:
				break;
			}
			action.put("tabId", tabId);
			actionsList.add(action);
		}
		return actionsList;
	}

	/**
	 * A private inner class to encapsulate the result of executing Playwright
	 * steps.
	 */
	private static class ExecutionResult {
		String newTabId;
		String newTabTitle;
		List<Map<String, Object>> originalTabActions = new ArrayList<>();
	}

	@Override
	public String getReactorDescription() {
		return "Reactor that replays step that is in order to run by given , sesionId, tabId, and fileName";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		} else if (key.equals("fileName")) {
			return "the name of the recorder file";
		} else if (key.equals("executeAll")) {
			return "Boolean that decide if you need to  execute all the remaining steps";
		} else if (key.equals("tabId")) {
			return "The id of the current tab of the playwright";
		}
		return super.getDescriptionForKey(key);
	}
}
