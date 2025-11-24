package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ReplaySingleStepReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ReplaySingleStepReactor.class);

	static ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

	public ReplaySingleStepReactor() {
		this.keysToGet = new String[] { "sessionId", "fileName", ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), "stepId",
				"tabId", ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 1, 0, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String sessionId = this.keyValue.get(this.keysToGet[0]);
		String fileName = this.keyValue.get(this.keysToGet[1]);
		Map<String, Object> inputs = getMap(this.keysToGet[2]);
		int stepId = Integer.parseInt(this.keyValue.get(this.keysToGet[3]));
		String tabId = this.keyValue.get(this.keysToGet[4]);
		String projectId = this.keyValue.get(this.keysToGet[5]);

		Map<String, Object> response = replayStep(sessionId, fileName, stepId, inputs, tabId, projectId);

		return new NounMetadata(response, PixelDataType.MAP);
	}

	public Map<String, Object> replayStep(String sessionId, String fileName, int stepId, Map<String, Object> inputs,
			String tabId, String projectId) {

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
				classLogger.warn("Provided tabId " + tabId + " doesn't match step's tabId " + actualTabId);
			}

			classLogger
					.info("Found step " + stepId + " in tab " + actualTabId + ": " + json.valueToTree(step).toString());

			// Get session
			PlaywrightSession s = this.insight.getUser().getPlaywrightSession(sessionId);
			if (s == null) {
				response.put("status", "failed");
				response.put("error", "Session not found");
				return response;
			}

			// Validate step can be executed
			String validationError = validateStep(s, step, inputs, actualTabId);
			if (validationError != null) {
				response.put("status", "failed");
				response.put("error", validationError);
				response.put("stepId", stepId);
				response.put("tabId", actualTabId);

				ScreenshotResponse screenshot = ScreenshotReactor.screenshot(s, actualTabId);
				response.put("screenshot", screenshot);

				return response;
			}

			// Execute the step and capture result
			PlaywrightStep stepToExecute = step;
			if (step.type() == PlaywrightStepType.TYPE && inputs != null && inputs.containsKey(step.label())) {
				stepToExecute = new PlaywrightStep(step, inputs.get(step.label()).toString());
			}

			Map<String, Object> executionResult = PlaywrightSessionUtility.applyStep(s, stepToExecute, actualTabId);

			// Get screenshot after execution attempt
			ScreenshotResponse screenshot = ScreenshotReactor.screenshot(s, actualTabId);
			response.put("screenshot", screenshot);

			if (executionResult != null) {
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
					classLogger.info("Step created new tab: " + newTabId);
				} else {
					response.put("isNewTab", false);
				}

				if (step.type() == PlaywrightStepType.NAVIGATE) {
					response.put("tabTitle", tabTitle);
				}
			} else {
				response.put("status", "failed");
				response.put("error", "Step execution failed");
				response.put("stepId", stepId);
				response.put("tabId", actualTabId);
				response.put("isNewTab", false);
			}

		} catch (Exception e) {
			classLogger.error("Error replaying step " + stepId, e);
			response.put("status", "failed");
			response.put("error", e.getMessage());
			response.put("isNewTab", false);

			// Try to get screenshot even on exception
			try {
				PlaywrightSession s = this.insight.getUser().getPlaywrightSession(sessionId);
				if (s != null) {
					String actualTabId = tabId != null && !tabId.isEmpty() ? tabId : "tab-1";
					if (s.tabPages.containsKey(actualTabId)) {
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

	private PlaywrightStep findNavigateStepForTab(Map<String, List<List<PlaywrightStep>>> allStepsMap, String tabId,
			int currentStepId) {
		List<List<PlaywrightStep>> pages = allStepsMap.get(tabId);
		if (pages == null || pages.isEmpty()) {
			return null;
		}

		// Find which page the current step belongs to
		int currentPageIndex = -1;
		for (int i = 0; i < pages.size(); i++) {
			List<PlaywrightStep> page = pages.get(i);
			for (PlaywrightStep step : page) {
				if (step.id() == currentStepId) {
					currentPageIndex = i;
					break;
				}
			}
			if (currentPageIndex != -1) {
				break;
			}
		}

		if (currentPageIndex == -1) {
			return null;
		}

		// Get the NAVIGATE step for the current page (should be first step)
		List<PlaywrightStep> currentPage = pages.get(currentPageIndex);
		if (!currentPage.isEmpty() && currentPage.get(0).type() == PlaywrightStepType.NAVIGATE) {
			return currentPage.get(0);
		}

		return null;
	}

	private String normalizeUrl(String url) {
		// Remove trailing slashes and convert to lowercase
		String normalized = url.toLowerCase().trim();
		while (normalized.endsWith("/")) {
			normalized = normalized.substring(0, normalized.length() - 1);
		}

		// Remove protocol for comparison
		if (normalized.startsWith("https://")) {
			normalized = normalized.substring(8);
		} else if (normalized.startsWith("http://")) {
			normalized = normalized.substring(7);
		}

		// Remove www. for comparison
		if (normalized.startsWith("www.")) {
			normalized = normalized.substring(4);
		}

		return normalized;
	}

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

	private String validateStep(PlaywrightSession s, PlaywrightStep step, Map<String, Object> inputs, String tabId) {
		// Check if page exists for the tab
		if (!s.tabPages.containsKey(tabId)) {
			return "Tab not found: " + tabId;
		}

		switch (step.type()) {
		case TYPE:
			// Check if input is required but not provided
			if (step.storeValue()) {
				if (inputs == null || !inputs.containsKey(step.label())) {
					return "Missing required input for field: " + step.label();
				}
			}
			// Validate selector can be found
			if (step.coords() != null && !canFindElement(s, step, tabId)) {
				return "Element not found or not ready for selector at coordinates: " + step.coords();
			}
			break;

		case CLICK:
			// Validate element is clickable
			if (step.coords() == null) {
				return "Missing coordinates for click";
			}
			if (!canFindElement(s, step, tabId)) {
				return "Element not found or not ready at coordinates: " + step.coords();
			}
			break;

		case NAVIGATE:
			if (step.url() == null || step.url().isEmpty()) {
				return "Missing URL for navigation";
			}
			break;

		case SCROLL:
			if (step.deltaY() == null) {
				return "Missing scroll delta";
			}
			break;

		case WAIT:
			if (step.waitAfterMs() == null) {
				return "Missing wait duration";
			}
			break;

		case CONTEXT:
			// Context steps don't need validation
			break;

		default:
			return "Unknown step type: " + step.type();
		}

		return null;
	}

	private boolean canFindElement(PlaywrightSession s, PlaywrightStep step, String tabId) {
		try {
			// Try to probe the element to see if it exists and matches step selector
			ElementProbeResponse probe = ProbeElementReactor.probeElementAt(s, step.coords(), tabId);
			Selector stepProbe = step.selector();
			return probe != null && PlaywrightUtility.matchesSelector(stepProbe, probe);
		} catch (Exception e) {
			classLogger.warn("Could not find element at coordinates: " + step.coords(), e);
			return false;
		}
	}

	private static class StepLocation {
		PlaywrightStep step;
		String tabId;

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