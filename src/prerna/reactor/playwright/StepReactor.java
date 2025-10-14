package prerna.reactor.playwright;

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean; 

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.playwright.JSHandle;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.PlaywrightException;

import prerna.reactor.AbstractReactor;
import prerna.reactor.playwright.SessionUtility;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;


public class StepReactor extends AbstractReactor {
	
	public StepReactor(){
		this.keysToGet = new String[] {
				"sessionId",
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
		
    	Map<String, Object> paramValues = Utility.getMap(this.store, this.curRow);
		
		Step step = json.convertValue(paramValues, Step.class);
        return new NounMetadata(executeStep(sessionId, step), PixelDataType.MAP);
	}
	
	public ScreenshotResponse executeStep(String sessionId, Step step) {
		Session s = SessionReactor.get(sessionId);
        boolean isPageChanged = SessionUtility.applyStep(s, step);
	    System.out.println("IsPageChanged: " + isPageChanged);
        addStepToHistory(s, step, isPageChanged);
        return ScreenshotReactor.screenshot(sessionId);
    }
	
	private void addStepToHistory(Session s, Step step, boolean isPageChanged) {
		String shouldStoreParam = this.keyValue.get(this.keysToGet[1]);
		boolean shouldStore = Boolean.parseBoolean(shouldStoreParam);
		Step newStep = step;
		
		if (!shouldStore && step.type() == StepType.TYPE) {
			newStep = new Step(step.type(),step.url(), step.coords(), "", step.pressEnter(), step.deltaY(), step.waitUntil(), step.waitAfterMs(), step.viewport(), step.timestamp(), step.label(), step.isPassword(), step.storeValue());
		}
		
		if(isPageChanged) {
			s.history.steps().add(new ArrayList<>(List.of(newStep)));
		} else {
			if(s.history.steps().isEmpty() || s.history.steps().size() <= 1) //If size is 1, add new list of steps as navigate is always the first step, should be in a separate list
				s.history.steps().add(new ArrayList<>(List.of(newStep)));
			else
				s.history.steps().getLast().add(newStep);
		}
	}
}
