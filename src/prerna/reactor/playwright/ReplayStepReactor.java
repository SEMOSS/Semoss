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

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ReplayStepReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ReplayStepReactor.class);

	static ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
	static Insight insightObj;
	Browser browser;
	Map<String, Object> response = new HashMap<>();
	Path recordingsDir = null;
	String projectId = null;

	public ReplayStepReactor() {
		this.keysToGet = new String[] { "sessionId", "fileName", ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				"executeAll", "tabId", ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 1 };
		insightObj = this.insight;
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String name = this.keyValue.get(this.keysToGet[1]);
		Map<String, Object> inputs = getMap(this.keysToGet[2]);
		String tabId = this.keyValue.get(this.keysToGet[4]);

		projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		recordingsDir = PlaywrightUtility.initRecordingsDir(projectId);

		ScreenshotResponse screenshot = replayFromFile(inputs, name, tabId);
		response.put("screenshot", screenshot);

		return new NounMetadata(response, PixelDataType.MAP);
	}

	public ScreenshotResponse replayFromFile(Map<String, Object> inputs, String nameOrPath, String tabId) {
		StepsEnvelope env = PlaywrightUtility.loadStepsFromFile(projectId, nameOrPath);
		return replay(env, inputs, tabId);
	}

	public ScreenshotResponse replay(StepsEnvelope steps, Map<String, Object> inputs, String tabId) {
		boolean executeAll = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[3]));

		Map<String, List<List<PlaywrightStep>>> allStepsMap = steps.steps();
		String requestedTabId = (tabId != null && !tabId.isEmpty()) ? tabId : "tab-1";
		List<List<PlaywrightStep>> allStepsList = allStepsMap.getOrDefault(requestedTabId, new ArrayList<>());

		classLogger.info("Loaded steps: " + json.valueToTree(steps).toString());

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
		PlaywrightSession s = (sessionId != null) ? this.insight.getUser().getPlaywrightSession(sessionId) : null;

		if (s == null) {
			// Create a new page within the shared context and a new Session
			Page page = ctx.newPage();
			// Align page viewport to steps if needed (context viewport is fixed, page can
			// adjust)
			try {
				page.setViewportSize(width, height);
			} catch (Exception e) {
				classLogger.warn("Failed to set page viewport to {}x{}: {}", width, height, e.getMessage());
			}
			s = new PlaywrightSession(ctx, page);

			if (s.history.meta() == null) {
				s.history = new StepsEnvelope("1.0", PlaywrightSession.newMeta(""), s.history.steps());
			}
			// Use provided sessionId if present; otherwise generate one
			String newId = (sessionId != null && !sessionId.isEmpty()) ? sessionId
					: java.util.UUID.randomUUID().toString();
			s.setUserAndSessionId(this.insight.getUser(), newId);
			this.insight.getUser().setPlaywrightSession(newId, s);
			sessionId = newId;
			classLogger.info("Created new Session in shared context with id: {}", sessionId);
		} else {
			// Optional: update viewport on existing page to match steps
			try {
				s.getPage().setViewportSize(width, height);
			} catch (Exception e) {
				classLogger.debug("Viewport update on existing page skipped/failed: {}", e.getMessage());
			}
		}
		ExecutionResult execResult = executeSteps(s, allStepsMap, requestedTabId, executeAll, inputs);

		String responseTabId = execResult.newTabId != null ? execResult.newTabId : requestedTabId;

		if (execResult.newTabId != null) {
			response.put("isNewTab", true);
			response.put("newTabId", execResult.newTabId);
			response.put("tabTitle", execResult.newTabTitle);
			response.put("originalTabId", requestedTabId);
			response.put("originalTabActions", execResult.originalTabActions);
			classLogger.info("New tab opened: " + execResult.newTabId + ", original tab has "
					+ execResult.originalTabActions.size() + " remaining actions");
		}

		// Get next actions for the response tab
		List<Map<String, Object>> nextActions = getNextActions(s, allStepsMap, responseTabId);
		response.put("actions", nextActions);

		// Calculate isLastPage
		boolean isLastPage = calculateIsLastPage(s, allStepsMap, responseTabId);
		response.put("isLastPage", isLastPage);
		s.isLastPage = isLastPage;

		classLogger.info("Returning " + nextActions.size() + " actions for tab: " + responseTabId);

		return ScreenshotReactor.screenshot(s, responseTabId);
	}

	private ExecutionResult executeSteps(PlaywrightSession s, Map<String, List<List<PlaywrightStep>>> allStepsMap,
			String tabId, boolean executeAll, Map<String, Object> inputs) {

		ExecutionResult result = new ExecutionResult();
		List<List<PlaywrightStep>> tabSteps = allStepsMap.get(tabId);

		if (tabSteps == null || tabSteps.isEmpty()) {
			return result;
		}

		if (s.getCurrentPageIndex(tabId) == 0 && s.getCurrentStepIndex(tabId) == 0) {
			PlaywrightStep navigateStep = tabSteps.get(0).get(0);
			Map<String, Object> stepResult = PlaywrightSessionUtility.applyStep(s, navigateStep, tabId);
			result.newTabTitle = (String) stepResult.get("tabTitle");
			result.newTabId = tabId;
			s.incrementPageIndex(tabId);
			classLogger.info("Executed initial NAVIGATE step for tab: " + tabId);
			return result;
		}

		if (executeAll) {
			return executeAllSteps(s, allStepsMap, tabId, inputs);
		} else {
			return executeSingleStep(s, allStepsMap, tabId, inputs);
		}
	}

	private ExecutionResult executeSingleStep(PlaywrightSession s, Map<String, List<List<PlaywrightStep>>> allStepsMap,
			String tabId, Map<String, Object> inputs) {

		ExecutionResult result = new ExecutionResult();
		List<List<PlaywrightStep>> tabSteps = allStepsMap.get(tabId);

		int pageIdx = s.getCurrentPageIndex(tabId);
		int stepIdx = s.getCurrentStepIndex(tabId);

		if (pageIdx >= tabSteps.size()) {
			classLogger.warn("PageIndex out of bounds for tab " + tabId);
			return result;
		}

		List<PlaywrightStep> currentPage = tabSteps.get(pageIdx);
		if (stepIdx >= currentPage.size()) {
			classLogger.warn("StepIndex out of bounds for tab " + tabId);
			return result;
		}

		PlaywrightStep step = currentPage.get(stepIdx);
		classLogger.info("Executing step: " + json.valueToTree(step).toString());

		// Check if step should be executed
		if (!step.shouldRun()) {
			classLogger.info("Skipping step (shouldRun=false): " + step.id());
			s.incrementStepIndex(tabId);

			// Move to next page if needed
			if (s.getCurrentStepIndex(tabId) >= currentPage.size()) {
				if (pageIdx < tabSteps.size() - 1) {
					s.incrementPageIndex(tabId);
					s.setCurrentStepIndex(tabId, 0);
					classLogger.info("Moving to next page for tab " + tabId);
				}
			}
			return result;
		}

		// Apply the step
		if (step.type() == PlaywrightStepType.TYPE && inputs != null && inputs.containsKey(step.label())) {
			PlaywrightStep newStep = new PlaywrightStep(step, inputs.get(step.label()).toString());
			PlaywrightSessionUtility.applyStep(s, newStep, tabId);
		} else {
			PlaywrightSessionUtility.applyStep(s, step, tabId);
		}

		// Increment step index
		s.incrementStepIndex(tabId);

		// Check if we need to move to next page
		if (s.getCurrentStepIndex(tabId) >= currentPage.size()) {
			if (pageIdx < tabSteps.size() - 1) {
				s.incrementPageIndex(tabId);
				s.setCurrentStepIndex(tabId, 0);
				classLogger.info("Moving to next page for tab " + tabId);
			}
		}

		// Handle new tab trigger
		if (step.isTriggerNewTab() != null && step.isTriggerNewTab().isTrue()) {
			String newTabId = step.isTriggerNewTab().tabId();
			result.newTabId = newTabId;

			// Initialize new tab indices
			if (!s.tabCurrentPageIndex.containsKey(newTabId)) {
				s.setCurrentPageIndex(newTabId, 0);
				s.setCurrentStepIndex(newTabId, 0);
			}

			// Get title
			Page newTabPage = s.tabPages.get(newTabId);
			result.newTabTitle = (newTabPage != null && newTabPage.title() != null
					&& !newTabPage.title().trim().isEmpty()) ? newTabPage.title() : newTabId;

			// Capture remaining actions for original tab
			result.originalTabActions = getNextActions(s, allStepsMap, tabId);

			classLogger.info("Step triggered new tab: " + newTabId);
		}

		return result;
	}

	private ExecutionResult executeAllSteps(PlaywrightSession s, Map<String, List<List<PlaywrightStep>>> allStepsMap,
			String tabId, Map<String, Object> inputs) {

		ExecutionResult result = new ExecutionResult();
		List<List<PlaywrightStep>> tabSteps = allStepsMap.get(tabId);

		int pageIdx = s.getCurrentPageIndex(tabId);
		if (pageIdx >= tabSteps.size()) {
			return result;
		}

		List<PlaywrightStep> currentPage = tabSteps.get(pageIdx);

		// Execute all steps on current page
		while (s.getCurrentStepIndex(tabId) < currentPage.size()) {
			PlaywrightStep step = currentPage.get(s.getCurrentStepIndex(tabId));

			// Check if step should be executed
			if (!step.shouldRun()) {
				classLogger.info("Skipping step (shouldRun=false): " + step.id());
				s.incrementStepIndex(tabId);
				continue;
			}

			if (step.type() == PlaywrightStepType.TYPE && inputs != null && inputs.containsKey(step.label())) {
				PlaywrightStep newStep = new PlaywrightStep(step, inputs.get(step.label()).toString());
				PlaywrightSessionUtility.applyStep(s, newStep, tabId);
			} else {
				PlaywrightSessionUtility.applyStep(s, step, tabId);
			}

			s.incrementStepIndex(tabId);

			// Handle new tab
			if (step.isTriggerNewTab() != null && step.isTriggerNewTab().isTrue()) {
				String newTabId = step.isTriggerNewTab().tabId();
				if (!s.tabCurrentPageIndex.containsKey(newTabId)) {
					s.setCurrentPageIndex(newTabId, 0);
					s.setCurrentStepIndex(newTabId, 0);
				}
				result.newTabId = newTabId;
				classLogger.info("Step triggered new tab during executeAll: " + newTabId);
			}
		}

		// Move to next page if not last
		if (pageIdx < tabSteps.size() - 1) {
			s.incrementPageIndex(tabId);
			s.setCurrentStepIndex(tabId, 0);
		}

		return result;
	}

	private List<Map<String, Object>> getNextActions(PlaywrightSession s,
			Map<String, List<List<PlaywrightStep>>> allStepsMap, String tabId) {
		List<List<PlaywrightStep>> tabSteps = allStepsMap.get(tabId);

		if (tabSteps == null || tabSteps.isEmpty()) {
			return new ArrayList<>();
		}

		int pageIdx = s.getCurrentPageIndex(tabId);
		int stepIdx = s.getCurrentStepIndex(tabId);

		if (pageIdx >= tabSteps.size()) {
			classLogger.info("No more pages for tab " + tabId);
			return new ArrayList<>();
		}

		List<PlaywrightStep> currentPage = tabSteps.get(pageIdx);
		return getPageActions(currentPage, stepIdx, tabId);
	}

	private boolean calculateIsLastPage(PlaywrightSession s, Map<String, List<List<PlaywrightStep>>> allStepsMap,
			String tabId) {
		List<List<PlaywrightStep>> tabSteps = allStepsMap.get(tabId);

		if (tabSteps == null || tabSteps.isEmpty()) {
			return true;
		}

		int pageIdx = s.getCurrentPageIndex(tabId);
		int stepIdx = s.getCurrentStepIndex(tabId);

		if (pageIdx >= tabSteps.size()) {
			return true;
		}

		boolean isLastPage = pageIdx == tabSteps.size() - 1;
		boolean completedAllSteps = stepIdx >= tabSteps.get(pageIdx).size();

		return isLastPage && completedAllSteps;
	}

	private static class ExecutionResult {
		String newTabId;
		String newTabTitle;
		List<Map<String, Object>> originalTabActions = new ArrayList<>();
	}

	public List<String> listRecordings() {
		try (var stream = Files.list(recordingsDir)) {
			return stream.filter(p -> p.getFileName().toString().endsWith(".json")).map(p -> p.getFileName().toString())
					.sorted().toList();
		} catch (Exception e) {
			throw new RuntimeException("Failed to list recordings", e);
		}
	}

	private List<Map<String, Object>> getPageActions(List<PlaywrightStep> steps, int currentStepIndex, String tabId) {
		List<Map<String, Object>> actionsList = new ArrayList<>();
		for (int i = currentStepIndex; i < steps.size(); i++) {
			Map<String, Object> action = new HashMap<>();
			PlaywrightStep current = steps.get(i);
			classLogger.info("Processing step for actions: " + json.valueToTree(current).toString());
			classLogger.info("coords: " + current.coords());
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
					Page page = s.tabPages.get(tabId);
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
			default:
				break;
			}
			action.put("tabId", tabId);
			actionsList.add(action);
		}
		return actionsList;
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