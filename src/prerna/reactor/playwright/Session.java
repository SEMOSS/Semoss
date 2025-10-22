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
    int currentStepIndex = 0;
    int currentPageIndex = 0;
    boolean isLastPage = false;

    Session(BrowserContext ctx, Page page) {
        this.ctx = ctx;
        tabPages.put("tab-1", page);
        history.steps().put("tab-1", new ArrayList<List<Step>>());
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

}
