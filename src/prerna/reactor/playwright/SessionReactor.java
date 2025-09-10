package prerna.reactor.playwright;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SessionReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(SessionReactor.class);
	private Browser browser;
    public final static Map<String, Session> sessions = new ConcurrentHashMap<>();

	@Override
	public NounMetadata execute() {
		
		Playwright pw = Playwright.create();
		browser = pw.chromium().launch(
	            new BrowserType.LaunchOptions().setHeadless(true));
	                    		
        return new NounMetadata(createAndOpen(), PixelDataType.MAP);
	}
	
	private String createAndOpen() {
        int width  = 1280;
        int height = 800;
        double dpr = 1.0;

        Browser.NewContextOptions ctxOps = new Browser.NewContextOptions()
                .setViewportSize(width, height)
                .setDeviceScaleFactor(dpr);

        BrowserContext ctx = browser.newContext(ctxOps);
        ctx.setDefaultTimeout(60_000);
        ctx.setDefaultNavigationTimeout(60_000);
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
        sessions.put(id, s);

        classLogger.info("Created playwright session successfully with id: {}", id);
        return id;
    }
	
    public static Session get(String id) {
        Session s = sessions.get(id);
        if (s == null) throw new RuntimeException("Unknown session: " + id);
        return s;
    }

}
