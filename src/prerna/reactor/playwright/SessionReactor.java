package prerna.reactor.playwright;

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
// ... existing code ...
import com.microsoft.playwright.Playwright;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// ... existing code ...
public class SessionReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(SessionReactor.class);
	private Browser browser;
   // public final static Map<String, Session> sessions = new ConcurrentHashMap<>();

	@Override
	public NounMetadata execute() {
		// Use a single Browser reused across the app
		browser = PlaywrightBrowserProvider.getBrowser();
		return new NounMetadata(createAndOpen(), PixelDataType.MAP);
	}
	
	private String createAndOpen() {
        int width  = 1280;
        int height = 800;
        double dpr = 1.0;

        // Get or create a shared BrowserContext for this user
        BrowserContext ctx = this.insight.getUser().getSharedPlaywrightContext();
        if (ctx == null) {
            Browser.NewContextOptions ctxOps = new Browser.NewContextOptions()
                    .setViewportSize(width, height)
                    .setDeviceScaleFactor(dpr);
            ctx = browser.newContext(ctxOps);
            ctx.setDefaultTimeout(60_000);
            ctx.setDefaultNavigationTimeout(60_000);
            this.insight.getUser().setSharedPlaywrightContext(ctx);
        }

        Page page = ctx.newPage();

        Session s = new Session(ctx, page);
        if (s.history.meta() == null) {
            s.history = new StepsEnvelope(
                    "1.0",
                    Session.newMeta(""),
                    s.history.steps()
            );
        }
        String id = UUID.randomUUID().toString();

        s.setUserAndSessionId(this.insight.getUser(), id);

        // Keep your existing API: store multiple sessions per user, all using the same context
        this.insight.getUser().setPlaywrightSession(id, s);

        classLogger.info("Created playwright session successfully with id: {}", id);
        return id;
    }

}