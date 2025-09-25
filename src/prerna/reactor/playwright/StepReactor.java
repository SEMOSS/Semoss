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
        boolean isPageChanged = applyStep(s, step);
	    System.out.println("IsPageChanged: " + isPageChanged);
        addStepToHistory(s, step, isPageChanged);
        return ScreenshotReactor.screenshot(sessionId);
    }
	
	public static boolean applyStep(Session s, Step step) {
	        Page page = s.page;

	        try {
	        	String before = page.url();

	    	    // Observe SPA page changes
	        	JSHandle mutationPromise = page.evaluateHandle(
	        		    "() => new Promise(resolve => {" +
	        		    "  const observer = new MutationObserver((muts) => {" +
	        		    "    for (const m of muts) {" +
	        		    "      if (m.type === 'childList' && (m.addedNodes.length > 0 || m.removedNodes.length > 0)) {" +
	        		    "        observer.disconnect();" +
	        		    "        resolve(true);" +
	        		    "        return;" +
	        		    "      }" +
	        		    "      if (m.type === 'characterData' && m.target.nodeValue.trim().length > 0) {" +
	        		    "        observer.disconnect();" +
	        		    "        resolve(true);" +
	        		    "        return;" +
	        		    "      }" +
	        		    "      if (m.type === 'attributes' && m.attributeName !== 'value') {" +
	        		    "        observer.disconnect();" +
	        		    "        resolve(true);" +
	        		    "        return;" +
	        		    "      }" +
	        		    "    }" +
	        		    "  });" +
	        		    "  observer.observe(document.body, { childList: true, subtree: true, attributes: true, characterData: true });" +
	        		    "  setTimeout(() => { observer.disconnect(); resolve(false); }, 1500);" +
	        		    "})"
	        		);
	        	
	        	AtomicBoolean networkTriggered = new AtomicBoolean(false);

	            page.onRequest(req -> {
	                if ("xhr".equals(req.resourceType()) || "fetch".equals(req.resourceType())) {
	                    networkTriggered.set(true);
	                }
	            });
	    	    
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
	                    // Select all existing text and delete it before typing new text
	                    page.keyboard().press("Control+A");
	                    page.keyboard().press("Delete");
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
	            
	            if (before.equals(page.url()) && !networkTriggered.get()) {
	        	    // Small buffer for SPA rendering
	        	    page.waitForTimeout(500);
		            return (detectPageChange(mutationPromise));
	            } else {
	            	return true;
	            }
	            
	        } catch (Exception e) {
	        	System.out.println("Failed to apply step: " + e);
	        	return true;
//	            throw new RuntimeException("Failed to apply step: " + step.type(), e);
	        }
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
	
	static boolean detectPageChange(JSHandle mutationPromise) {

		try {

	    // Resolve mutation observer promise
	    boolean domChanged = (boolean) mutationPromise.evaluate("value => value");
	    System.out.println("DOM Changed: " + domChanged);
	    return domChanged;
		} catch (Exception e) {
            throw new RuntimeException("Failed to evaluate DOM changes: " + e);
		}
	}
}
