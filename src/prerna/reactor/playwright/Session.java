package prerna.reactor.playwright;

import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Session {
	
	public final BrowserContext ctx;
    Map<String, Page> tabPages = new HashMap<>();
    StepsEnvelope history = new StepsEnvelope("1", newMeta(""), new HashMap<>());
    Map<String, Integer> tabCurrentPageIndex = new HashMap<>();
    Map<String, Integer> tabCurrentStepIndex = new HashMap<>();
    boolean isLastPage = false;

    Session(BrowserContext ctx, Page page) {
        this.ctx = ctx;
        tabPages.put("tab-1", page);
        history.steps().put("tab-1", new ArrayList<List<Step>>());

        tabCurrentPageIndex.put("tab-1", 0);
        tabCurrentStepIndex.put("tab-1", 0);
    }
    
    public static RecordingMeta newMeta(String maybeTitleOrUrl) {
        long now = System.currentTimeMillis();
        return new RecordingMeta(
                java.util.UUID.randomUUID().toString(),
                maybeTitleOrUrl,       // or null; you can set a better title later
                null,                  // description starts empty
                now,
                now
        );
    }

    public int getCurrentPageIndex(String tabId) {
        return tabCurrentPageIndex.getOrDefault(tabId, 0);
    }
    
    public void setCurrentPageIndex(String tabId, int index) {
        tabCurrentPageIndex.put(tabId, index);
    }
    
    public int getCurrentStepIndex(String tabId) {
        return tabCurrentStepIndex.getOrDefault(tabId, 0);
    }
    
    public void setCurrentStepIndex(String tabId, int index) {
        tabCurrentStepIndex.put(tabId, index);
    }
    
    public void incrementPageIndex(String tabId) {
        int current = getCurrentPageIndex(tabId);
        setCurrentPageIndex(tabId, current + 1);
    }
    
    public void incrementStepIndex(String tabId) {
        int current = getCurrentStepIndex(tabId);
        setCurrentStepIndex(tabId, current + 1);
    }

}
