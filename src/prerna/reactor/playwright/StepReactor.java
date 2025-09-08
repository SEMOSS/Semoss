package prerna.reactor.playwright;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.PlaywrightException;

import prerna.reactor.AbstractReactor;
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
        applyStep(s, step);
        addStepToHistory(s, step);
        return ScreenshotReactor.screenshot(sessionId);
    }
	
	public static void applyStep(Session s, Step step) {
	        Page page = s.page;

	        try {
	            switch (step.type()) {
	                case NAVIGATE -> {
	                    var opts = new Page.NavigateOptions()
	                            .setWaitUntil(com.microsoft.playwright.options.WaitUntilState.LOAD)
	                            .setTimeout(60_000);
	                    page.navigate(step.url(), opts);

	                    // Optional short polish: try network idle without blocking forever
	                    try {
	                        page.waitForLoadState(com.microsoft.playwright.options.LoadState.NETWORKIDLE,
	                                new Page.WaitForLoadStateOptions().setTimeout(4_000));
	                    } catch (PlaywrightException ignored) {}
	                }
	                case CLICK -> {
	                    String before = page.url();

	                    // Perform the click
	                    page.mouse().move(step.coords().x(), step.coords().y());
	                    page.mouse().click(step.coords().x(), step.coords().y());

	                    // If navigation happens, wait for it (URL changed)
	                    try {
	                        page.waitForURL(u -> !u.equals(before),
	                                new Page.WaitForURLOptions().setTimeout(6_000)); // waits if it navigates
	                    } catch (PlaywrightException ignored) {
	                        // No URL change -> stay on same page; that's fine
	                    }

	                    // Then wait for DOM content to be ready and visible
	                    try {
	                        page.waitForLoadState(com.microsoft.playwright.options.LoadState.LOAD,
	                                new Page.WaitForLoadStateOptions().setTimeout(3_000));
	                    } catch (PlaywrightException ignored) {
	                        // LOAD didn't come in time; continue (we'll still screenshot)
	                    }
	                }
	                case TYPE -> {
	                    page.mouse().click(step.coords().x(), step.coords().y());
	                    if (step.text() != null) page.keyboard().type(step.text());
	                    if (Boolean.TRUE.equals(step.pressEnter())) page.keyboard().press("Enter");
	                }
	                case SCROLL -> {
	                    int dy = step.deltaY() != null ? step.deltaY() : 300;
	                    page.mouse().wheel(0, dy);
	                }
	                case WAIT -> {
	                    int ms = step.waitAfterMs() != null ? step.waitAfterMs() : 300;
	                    page.waitForTimeout(ms);
	                }
	            }
	            if (step.waitAfterMs() != null && step.waitAfterMs() > 0) {
	                page.waitForTimeout(step.waitAfterMs());
	            }
	        } catch (Exception e) {
	            throw new RuntimeException("Failed to apply step: " + step.type(), e);
	        }
	    }
	
	private void addStepToHistory(Session s, Step step) {
		String shouldStoreParam = this.keyValue.get(this.keysToGet[1]);
		boolean shouldStore = Boolean.parseBoolean(shouldStoreParam);
		
		if (!shouldStore) {
			Step newStep = new Step(step.type(),step.url(), step.coords(), "", step.pressEnter(), step.deltaY(), step.waitUntil(), step.waitAfterMs(), step.viewport(), step.timestamp(), step.label(), step.isPassword(), step.storeValue());
			s.history.steps().add(newStep);
		} else {
			s.history.steps().add(step);
		}
	}

}
