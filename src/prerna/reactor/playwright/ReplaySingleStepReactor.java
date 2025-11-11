package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.playwright.Page;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ReplaySingleStepReactor extends AbstractReactor {

    static ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final Logger classLogger = LogManager.getLogger(ReplaySingleStepReactor.class);

    public ReplaySingleStepReactor(){
        this.keysToGet = new String[] {
                "sessionId",
                "fileName",
                ReactorKeysEnum.PARAM_VALUES_MAP.getKey(),
                "stepId",
                "tabId"
        };
        this.keyRequired = new int[] { 1, 1, 0, 1, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String sessionId = this.keyValue.get(this.keysToGet[0]);
        String fileName = this.keyValue.get(this.keysToGet[1]);
        Map<String, Object> inputs = getMap(this.keysToGet[2]);
        int stepId = Integer.parseInt(this.keyValue.get(this.keysToGet[3]));
        String tabId = this.keyValue.get(this.keysToGet[4]);

        Map<String, Object> response = replayStep(sessionId, fileName, stepId, inputs, tabId);

        return new NounMetadata(response, PixelDataType.MAP);
    }

    public Map<String, Object> replayStep(String sessionId, String fileName,
                                          int stepId, Map<String, Object> inputs, String tabId) {

        Map<String, Object> response = new HashMap<>();

        try {
            // Load the steps file
            StepsEnvelope env = PlaywrightUtility.loadStepsFromFile(fileName);
            Map<String, List<List<Step>>> allStepsMap = env.steps();

            // Find the step by ID
            StepLocation location = findStepById(allStepsMap, stepId);

            if (location == null) {
                response.put("status", "failed");
                response.put("error", "Step with ID " + stepId + " not found");
                return response;
            }

            Step step = location.step;
            String actualTabId = location.tabId;

            // If tabId was provided, verify it matches
            if (tabId != null && !tabId.isEmpty() && !tabId.equals(actualTabId)) {
                classLogger.warn("Provided tabId " + tabId + " doesn't match step's tabId " + actualTabId);
            }

            classLogger.info("Found step " + stepId + " in tab " + actualTabId + ": " +
                    json.valueToTree(step).toString());

            // Get session
            Session s = this.insight.getUser().getPlaywrightSession(sessionId);
            if (s == null) {
                response.put("status", "failed");
                response.put("error", "Session not found");
                return response;
            }

            // Validate the session is on the correct page for this step
//            String pageValidationError = validatePageState(s, step, allStepsMap, actualTabId, stepId);
//            if (pageValidationError != null) {
//                response.put("status", "failed");
//                response.put("error", pageValidationError);
//                response.put("stepId", stepId);
//                response.put("tabId", actualTabId);
//
//                // Still return screenshot
//                if (s.tabPages.containsKey(actualTabId)) {
//                    ScreenshotResponse screenshot = ScreenshotReactor.screenshot(s, actualTabId);
//                    response.put("screenshot", screenshot);
//                }
//
//                return response;
//            }

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
            Step stepToExecute = step;
            if (step.type() == StepType.TYPE && inputs != null && inputs.containsKey(step.label())) {
                stepToExecute = new Step(step, inputs.get(step.label()).toString());
            }

            Map<String, Object> executionResult = SessionUtility.applyStep(s, stepToExecute, actualTabId);

            // Get screenshot after execution attempt
            ScreenshotResponse screenshot = ScreenshotReactor.screenshot(s, actualTabId);
            response.put("screenshot", screenshot);

            if (executionResult != null) {
                response.put("status", "success");
                response.put("stepId", stepId);
                response.put("tabId", actualTabId);

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
                Session s = this.insight.getUser().getPlaywrightSession(sessionId);
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


    private String validatePageState(Session s, Step step, Map<String, List<List<Step>>> allStepsMap,
                                     String tabId, int stepId) {

        // Get the page for this tab
        Page page = s.tabPages.get(tabId);
        if (page == null) {
            return "Tab " + tabId + " does not exist in session. Please execute the NAVIGATE step first.";
        }

        // Find the NAVIGATE step for the current page
        Step navigateStep = findNavigateStepForTab(allStepsMap, tabId, stepId);

        if (navigateStep == null) {
            // If no NAVIGATE step exists, we can't validate the page state
            classLogger.warn("No NAVIGATE step found for current page in tab " + tabId);
            return null;
        }

        // Skip validation if this IS the navigate step
        if (step.type() == StepType.NAVIGATE) {
            return null;
        }

        // Get current URL
        String currentUrl = page.url();
        String expectedUrl = navigateStep.url();

        if (currentUrl == null || currentUrl.equals("about:blank")) {
            return "Page not loaded. Please execute step " + navigateStep.id() +
                    " (NAVIGATE to " + expectedUrl + ") first.";
        }

        // Check if we're on the correct domain/page
        if (!isSamePage(currentUrl, expectedUrl)) {
            return "Wrong page loaded. Expected to be on '" + expectedUrl +
                    "' but currently on '" + currentUrl + "'. Please execute step " +
                    navigateStep.id() + " (NAVIGATE) first.";
        }

        return null;
    }

    private Step findNavigateStepForTab(Map<String, List<List<Step>>> allStepsMap, String tabId, int currentStepId) {
        List<List<Step>> pages = allStepsMap.get(tabId);
        if (pages == null || pages.isEmpty()) {
            return null;
        }

        // Find which page the current step belongs to
        int currentPageIndex = -1;
        for (int i = 0; i < pages.size(); i++) {
            List<Step> page = pages.get(i);
            for (Step step : page) {
                if (step.id() == currentStepId) {
                    currentPageIndex = i;
                    break;
                }
            }
            if (currentPageIndex != -1) break;
        }

        if (currentPageIndex == -1) {
            return null;
        }

        // Get the NAVIGATE step for the current page (should be first step)
        List<Step> currentPage = pages.get(currentPageIndex);
        if (!currentPage.isEmpty() && currentPage.get(0).type() == StepType.NAVIGATE) {
            return currentPage.get(0);
        }

        return null;
    }

    private boolean isSamePage(String currentUrl, String expectedUrl) {
        try {
            // Normalize URLs for comparison
            String currentNormalized = normalizeUrl(currentUrl);
            String expectedNormalized = normalizeUrl(expectedUrl);

            // Check if the current URL starts with the expected URL (allows for same page with different query params)
            return currentNormalized.startsWith(expectedNormalized) ||
                    currentNormalized.equals(expectedNormalized);
        } catch (Exception e) {
            classLogger.error("Error comparing URLs", e);
            return false;
        }
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

    private StepLocation findStepById(Map<String, List<List<Step>>> allStepsMap, int stepId) {
        for (Map.Entry<String, List<List<Step>>> entry : allStepsMap.entrySet()) {
            String tabId = entry.getKey();
            List<List<Step>> pages = entry.getValue();

            for (List<Step> page : pages) {
                for (Step step : page) {
                    if (step.id() == stepId) {
                        return new StepLocation(step, tabId);
                    }
                }
            }
        }
        return null;
    }

    private String validateStep(Session s, Step step, Map<String, Object> inputs, String tabId) {
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

    private boolean canFindElement(Session s, Step step, String tabId) {
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
        Step step;
        String tabId;

        StepLocation(Step step, String tabId) {
            this.step = step;
            this.tabId = tabId;
        }
    }
}