package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import prerna.reactor.AbstractReactor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;


public class StepReactor extends AbstractReactor {

    private final static String REACTOR_DESCRIPTION = "Execute a step in the current page of the playwright session.";
    private final static String SESSION_ID_KEY_DESCRIPTION = "Playwright session ID that stores information about the history of actions done during that session";
    private final static String SHOULD_STORE_KEY_DESCRIPTION = "Boolean flag to indicate whether to store the value of TYPE actions in the session history. If false, the value will be replaced with an empty string.";
    private final static String INPUTS_KEY_DESCRIPTION = "Map of step parameters. Required keys: type (NAVIGATE, CLICK, TYPE, SCROLL, WAIT), url (for NAVIGATE), coords (for CLICK and TYPE), text (for TYPE), pressEnter (for TYPE), deltaY (for SCROLL), waitAfterMs (optional for all types).";
    Map<String, Object> response = new HashMap<>();

    public StepReactor() {
        this.keysToGet = new String[]{
                "sessionId",
                "tabId",
                "shouldStore",
                ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[]{1, 1, 0, 1};
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        String sessionId = this.keyValue.get(this.keysToGet[0]);
        String tabId = this.keyValue.get(this.keysToGet[1]);

        Map<String, Object> paramValues = getMap(this.keysToGet[3]);

        Step step = json.convertValue(paramValues, Step.class);
        ScreenshotResponse screenshotResponse = step.type() == StepType.CONTEXT ?
                executeContextStep(sessionId, step, tabId) : executeStep(sessionId, step, tabId);

        response.put("screenshot", screenshotResponse);

        return new NounMetadata(response, PixelDataType.MAP);
    }

    public ScreenshotResponse executeContextStep(String sessionId, Step step, String tabId) {
        Session s = this.insight.getUser().getPlaywrightSession(sessionId);

        int stepId = ++s.lastStepId;
        Step newStep = new Step(step, stepId);

        if (newStep.multiCoords().isEmpty() || newStep.prompt().isEmpty()) {
            throw new IllegalArgumentException("CONTEXT step requires multiCoords and prompt to be non-empty.");
        }

        if (s.history.steps().isEmpty() || s.history.steps().size() <= 1) {
            s.history.steps().get(tabId).add(new ArrayList<>(List.of(newStep)));
        } else {
            s.history.steps().get(tabId).getLast().add(newStep);
        }

        response.put("stepId", stepId);
        return ScreenshotReactor.screenshot(s, tabId);
    }

    public ScreenshotResponse executeStep(String sessionId, Step step, String tabId) {
        Session s = this.insight.getUser().getPlaywrightSession(sessionId);
        Map<String, Object> stepResult = SessionUtility.applyStep(s, step, tabId);
        boolean isPageChanged = (Boolean) stepResult.get("isPageChanged");
        boolean isNewTab = (Boolean) stepResult.get("isNewTab");

        String newTabId = null;

        // If a new tab was opened, capture the new tab ID
        if (isNewTab) {
            newTabId = (String) stepResult.get("newTabId");
        }

        addStepToHistory(s, step, isPageChanged, tabId, isNewTab, newTabId);
        response.put("isNewTab", isNewTab);

        if (newTabId != null) {
            response.put("newTabId", newTabId);
        }

        if (stepResult.get("tabTitle") != null) {
            response.put("tabTitle", stepResult.get("tabTitle"));
        }

        // Return screenshot from the NEW tab if one was opened, otherwise from the current tab
        String screenshotTabId = isNewTab && newTabId != null ? newTabId : tabId;
        return ScreenshotReactor.screenshot(this.insight.getUser().getPlaywrightSession(sessionId), screenshotTabId);
    }

    private void addStepToHistory(Session s, Step step, boolean isPageChanged, String tabId,
                                  boolean isNewTab, String newTabId) {
        String shouldStoreParam = this.keyValue.get(this.keysToGet[2]);
        boolean shouldStore = Boolean.parseBoolean(shouldStoreParam);
        int stepId = ++s.lastStepId;
        Step newStep = new Step(step, stepId);

        if (!shouldStore && step.type() == StepType.TYPE) {
            newStep = new Step(stepId, step.type(), step.url(), step.coords(), step.multiCoords(), step.prompt(), "", step.pressEnter(),
                    step.deltaY(), step.waitUntil(), step.waitAfterMs(), step.viewport(), step.timestamp(), step.label(), step.description(),
                    step.isPassword(), step.storeValue(), step.selector(), step.isTriggerNewTab(), step.shouldRun(), step.required()
            );
        }

        if (isNewTab && newTabId != null) {
            TriggerNewTab triggerNewTab = new TriggerNewTab(true, newTabId);
            newStep = new Step(
                    stepId, newStep.type(), newStep.url(), newStep.coords(), step.multiCoords(), step.prompt(), newStep.text(),
                    newStep.pressEnter(), newStep.deltaY(), newStep.waitUntil(),
                    newStep.waitAfterMs(), newStep.viewport(), newStep.timestamp(),
                    newStep.label(), newStep.description(), newStep.isPassword(), newStep.storeValue(),
                    newStep.selector(), triggerNewTab, newStep.shouldRun(), newStep.required()
            );
            s.addChildTabRelationship(tabId, newTabId);
        }

        if (isPageChanged) {
            s.history.steps().get(tabId).add(new ArrayList<>(List.of(newStep)));
        } else {
            if (s.history.steps().isEmpty() || s.history.steps().size() <= 1) {
                s.history.steps().get(tabId).add(new ArrayList<>(List.of(newStep)));
            } else {
                s.history.steps().get(tabId).getLast().add(newStep);
            }
        }
        response.put("stepId", stepId);
    }

    @Override
    public String getReactorDescription() {
        return REACTOR_DESCRIPTION;
    }

    @Override
    protected String getDescriptionForKey(String key) {
        return switch (key) {
            case "sessionId" -> SESSION_ID_KEY_DESCRIPTION;
            case "shouldStore" -> SHOULD_STORE_KEY_DESCRIPTION;
            case "paramValues" -> INPUTS_KEY_DESCRIPTION;
            default -> super.getDescriptionForKey(key);
        };
    }
}