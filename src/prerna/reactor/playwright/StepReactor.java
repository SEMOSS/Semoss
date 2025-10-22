package prerna.reactor.playwright;

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

	public StepReactor(){
		this.keysToGet = new String[] {
				"sessionId",
                "tabId",
				"shouldStore",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
				};
		this.keyRequired = new int[] { 1, 0, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
	    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		String sessionId = this.keyValue.get(this.keysToGet[0]);
        String tabId = this.keyValue.get(this.keysToGet[1]);

        Map<String, Object> paramValues = getMap(this.keysToGet[3]);
		
		Step step = json.convertValue(paramValues, Step.class);
        return new NounMetadata(executeStep(sessionId, step, tabId), PixelDataType.MAP);
	}
	
	public ScreenshotResponse executeStep(String sessionId, Step step, String tabId) {
		Session s = this.insight.getUser().getPlaywrightSession(sessionId);
        boolean isPageChanged = SessionUtility.applyStep(s, step, tabId);
        addStepToHistory(s, step, isPageChanged, tabId);
        return ScreenshotReactor.screenshot(this.insight.getUser().getPlaywrightSession(sessionId));
    }
	
	private void addStepToHistory(Session s, Step step, boolean isPageChanged, String tabId) {
		String shouldStoreParam = this.keyValue.get(this.keysToGet[1]);
		boolean shouldStore = Boolean.parseBoolean(shouldStoreParam);
		Step newStep = step;
		
		if (!shouldStore && step.type() == StepType.TYPE) {
			newStep = new Step(step.type(),step.url(), step.coords(), "", step.pressEnter(), step.deltaY(), step.waitUntil(), step.waitAfterMs(), step.viewport(), step.timestamp(), step.label(), step.isPassword(), step.storeValue(), step.selector());
		}
		
		if(isPageChanged) {
			s.history.steps().get(tabId).add(new ArrayList<>(List.of(newStep)));
		} else {
			if(s.history.steps().isEmpty() || s.history.steps().size() <= 1) //If size is 1, add new list of steps as navigate is always the first step, should be in a separate list
				s.history.steps().get(tabId).add(new ArrayList<>(List.of(newStep)));
			else
				s.history.steps().get(tabId).getLast().add(newStep);
		}
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
