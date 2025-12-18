package prerna.reactor.playwright;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class StepReactor extends AbstractReactor {

	private ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
	private Map<String, Object> response = new HashMap<>();

	/**
	 * Default constructor for StepReactor. Initializes the keys this reactor
	 * expects: sessionId, tabId, shouldStore, and paramValues.
	 */
	public StepReactor() {
		this.keysToGet = new String[] { "sessionId", "tabId", "shouldStore",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 1 };
	}

	/**
	 * Executes a single Playwright step within an active session and records it.
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

		PlaywrightStep step = json.convertValue(paramValues, PlaywrightStep.class);
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
	 * creation and page change logic.
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
					step.tag());
		}

		if (isNewTab && newTabId != null) {
			TriggerNewTab triggerNewTab = new TriggerNewTab(true, newTabId);
			newStep = new PlaywrightStep(stepId, newStep.type(), newStep.url(), newStep.coords(), step.multiCoords(),
					step.prompt(), newStep.text(), newStep.pressEnter(), newStep.deltaY(), newStep.waitUntil(),
					newStep.waitAfterMs(), newStep.viewport(), newStep.timestamp(), newStep.label(),
					newStep.description(), newStep.isPassword(), newStep.storeValue(), newStep.selector(),
					triggerNewTab, newStep.shouldRun(), newStep.required(), step.tag());
			playwrightSession.addChildTabRelationship(tabId, newTabId);
		}

		if (isPageChanged) {
			playwrightSession.history.steps().get(tabId).add(new ArrayList<>(List.of(newStep)));
		} else {
			if (playwrightSession.history.steps().isEmpty() || playwrightSession.history.steps().size() <= 1) {
				playwrightSession.history.steps().get(tabId).add(new ArrayList<>(List.of(newStep)));
			} else {
				playwrightSession.history.steps().get(tabId).getLast().add(newStep);
			}
		}
		response.put("stepId", stepId);
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
		}

		return super.getDescriptionForKey(key);
	}
}