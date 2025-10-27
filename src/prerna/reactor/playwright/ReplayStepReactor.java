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
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;


public class ReplayStepReactor extends AbstractReactor {

	public static Path recordingsDir = PlaywrightUtility.initRecordingsDir();
    static ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    static Insight insightObj;
	Browser browser;
	Map<String, Object> response = new HashMap<>();
	private static final Logger classLogger = LogManager.getLogger(ReplayStepReactor.class);
	
	public ReplayStepReactor(){
		this.keysToGet = new String[] {
				"sessionId",
				"fileName",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
				"executeAll",
				"tabId"  
		};
		this.keyRequired = new int[] { 1,1,0,0,0 };
		insightObj = this.insight;
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
    	Map<String, Object> inputs = getMap(this.keysToGet[2]);
		String name = this.keyValue.get(this.keysToGet[1]);
		String tabId = this.keyValue.get(this.keysToGet[4]);  
		ScreenshotResponse screenshot = replayFromFile(inputs, name, tabId); 
		response.put("screenshot", screenshot);
		
        return new NounMetadata(response, PixelDataType.MAP);
	}
	
    public ScreenshotResponse replayFromFile(Map<String, Object> inputs, String nameOrPath, String tabId) { 
        StepsEnvelope env = PlaywrightUtility.loadStepsFromFile(nameOrPath);
        return replay(env, inputs, tabId); 
    }
	
	public ScreenshotResponse replay(StepsEnvelope steps, Map<String, Object> inputs, String tabId) { 
		boolean executeAll = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[3]));
		
		Map<String, List<List<Step>>> allStepsMap = steps.steps();
		String requestedTabId = (tabId != null && !tabId.isEmpty()) ? tabId : "tab-1";
		List<List<Step>> allStepsList = allStepsMap.getOrDefault(requestedTabId, new ArrayList<>());

        classLogger.info("Loaded steps: " + json.valueToTree(steps).toString());

		Playwright pw = Playwright.create();
		browser = pw.chromium().launch(
	            new BrowserType.LaunchOptions().setHeadless(true));
        BrowserContext ctx = browser.newContext(new Browser.NewContextOptions()
                .setViewportSize(allStepsList.get(0).get(0).viewport().width(),
                		allStepsList.get(0).get(0).viewport().height())
                .setDeviceScaleFactor(allStepsList.get(0).get(0).viewport().deviceScaleFactor())
        );
        Page page = ctx.newPage();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		Session s = this.insight.getUser().getPlaywrightSession(sessionId);
	    
		ExecutionResult execResult = executeSteps(s, allStepsMap, requestedTabId, executeAll, inputs);
		
		String responseTabId = execResult.newTabId != null ? execResult.newTabId : requestedTabId;
		
		if (execResult.newTabId != null) {
			response.put("isNewTab", true);
			response.put("newTabId", execResult.newTabId);
			response.put("tabTitle", execResult.newTabTitle);
			response.put("originalTabId", requestedTabId);
			response.put("originalTabActions", execResult.originalTabActions);
			classLogger.info("New tab opened: " + execResult.newTabId + ", original tab has " + 
				execResult.originalTabActions.size() + " remaining actions");
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
	
	private ExecutionResult executeSteps(Session s, Map<String, List<List<Step>>> allStepsMap, 
			String tabId, boolean executeAll, Map<String, Object> inputs) {
		
		ExecutionResult result = new ExecutionResult();
		List<List<Step>> tabSteps = allStepsMap.get(tabId);
		
		if (tabSteps == null || tabSteps.isEmpty()) {
			return result;
		}
		
		if (s.getCurrentPageIndex(tabId) == 0 && s.getCurrentStepIndex(tabId) == 0) {
			Step navigateStep = tabSteps.get(0).get(0);
			SessionUtility.applyStep(s, navigateStep, tabId);
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
	
	private ExecutionResult executeSingleStep(Session s, Map<String, List<List<Step>>> allStepsMap,
			String tabId, Map<String, Object> inputs) {
		
		ExecutionResult result = new ExecutionResult();
		List<List<Step>> tabSteps = allStepsMap.get(tabId);
		
		int pageIdx = s.getCurrentPageIndex(tabId);
		int stepIdx = s.getCurrentStepIndex(tabId);
		
		if (pageIdx >= tabSteps.size()) {
			classLogger.warn("PageIndex out of bounds for tab " + tabId);
			return result;
		}
		
		List<Step> currentPage = tabSteps.get(pageIdx);
		if (stepIdx >= currentPage.size()) {
			classLogger.warn("StepIndex out of bounds for tab " + tabId);
			return result;
		}
		
		Step step = currentPage.get(stepIdx);
		classLogger.info("Executing step: " + json.valueToTree(step).toString());
		
		// Apply the step
		if (step.type() == StepType.TYPE && inputs != null && inputs.containsKey(step.label())) {
			Step newStep = new Step(step, inputs.get(step.label()).toString());
			SessionUtility.applyStep(s, newStep, tabId);
		} else {
			SessionUtility.applyStep(s, step, tabId);
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
			result.newTabTitle = (newTabPage != null && newTabPage.title() != null && !newTabPage.title().trim().isEmpty())
				? newTabPage.title() : newTabId;
			
			// Capture remaining actions for original tab
			result.originalTabActions = getNextActions(s, allStepsMap, tabId);
			
			classLogger.info("Step triggered new tab: " + newTabId);
		}
		
		return result;
	}
	
	private ExecutionResult executeAllSteps(Session s, Map<String, List<List<Step>>> allStepsMap,
			String tabId, Map<String, Object> inputs) {
		
		ExecutionResult result = new ExecutionResult();
		List<List<Step>> tabSteps = allStepsMap.get(tabId);
		
		int pageIdx = s.getCurrentPageIndex(tabId);
		if (pageIdx >= tabSteps.size()) {
			return result;
		}
		
		List<Step> currentPage = tabSteps.get(pageIdx);
		
		// Execute all steps on current page
		while (s.getCurrentStepIndex(tabId) < currentPage.size()) {
			Step step = currentPage.get(s.getCurrentStepIndex(tabId));
			
			if (step.type() == StepType.TYPE && inputs != null && inputs.containsKey(step.label())) {
				Step newStep = new Step(step, inputs.get(step.label()).toString());
				SessionUtility.applyStep(s, newStep, tabId);
			} else {
				SessionUtility.applyStep(s, step, tabId);
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
	
	private List<Map<String, Object>> getNextActions(Session s, Map<String, List<List<Step>>> allStepsMap, String tabId) {
		List<List<Step>> tabSteps = allStepsMap.get(tabId);
		
		if (tabSteps == null || tabSteps.isEmpty()) {
			return new ArrayList<>();
		}
		
		int pageIdx = s.getCurrentPageIndex(tabId);
		int stepIdx = s.getCurrentStepIndex(tabId);
		
		if (pageIdx >= tabSteps.size()) {
			classLogger.info("No more pages for tab " + tabId);
			return new ArrayList<>();
		}
		
		List<Step> currentPage = tabSteps.get(pageIdx);
		return getPageActions(currentPage, stepIdx, tabId);
	}
	
	private boolean calculateIsLastPage(Session s, Map<String, List<List<Step>>> allStepsMap, String tabId) {
		List<List<Step>> tabSteps = allStepsMap.get(tabId);
		
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
            return stream.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .map(p -> p.getFileName().toString())
                    .sorted()
                    .toList();
        } catch (Exception e) {
            throw new RuntimeException("Failed to list recordings", e);
        }
    }
	
	private List<Map<String, Object>> getPageActions(List<Step> steps, int currentStepIndex, String tabId) {  
		List<Map<String, Object>> actionsList = new ArrayList<>();
		for(int i=currentStepIndex; i<steps.size();i++) {
			Map<String, Object> action = new HashMap<>();
			Step current = steps.get(i);
			classLogger.info("Processing step for actions: " + json.valueToTree(current).toString());
			classLogger.info("coords: " + current.coords());
			switch (current.type()) {
			case TYPE:
				Map<String, Object> typeAction = new HashMap<>();
				typeAction.put("label", current.label());
				typeAction.put("text", current.text());
				typeAction.put("isPassword", current.isPassword());
				typeAction.put("coords", current.coords());
				
				try {
					String sessionId = this.keyValue.get(this.keysToGet[0]);
					ElementProbeResponse probeResult = ProbeElementReactor.probeElementAt(
						this.insight.getUser().getPlaywrightSession(sessionId), current.coords(), tabId);
					typeAction.put("probe", probeResult);
				} catch (Exception e) {
					System.err.println("Failed to probe element for TYPE action: " + e.getMessage());
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
			default:
				break;
			}
			action.put("tabId", tabId); 
			actionsList.add(action);
		}
		return actionsList;
	}	
}