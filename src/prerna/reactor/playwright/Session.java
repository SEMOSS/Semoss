package prerna.reactor.playwright;

import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;

public class Session {
	
	private final BrowserContext ctx;
    final Page page;
    StepsEnvelope history = new StepsEnvelope("1", newMeta(""), new java.util.ArrayList<>());

    Session(BrowserContext ctx, Page page) {
        this.ctx = ctx; this.page = page;
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
