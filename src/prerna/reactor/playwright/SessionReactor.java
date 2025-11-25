package prerna.reactor.playwright;

import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SessionReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(SessionReactor.class);

	// Synchronization lock for thread-safe context creation
	private static final Object CONTEXT_CREATION_LOCK = new Object();

	@Override
	public NounMetadata execute() {
		Browser browser = PlaywrightBrowserProvider.getBrowser();

		int width = 1280;
		int height = 800;
		double dpr = 1.0;

		BrowserContext ctx = this.insight.getUser().getSharedPlaywrightContext();
		Page page = null;

		// Try to use existing context, or create new one if needed
		if (ctx != null) {
			try {
				// Validate context is still alive before using it
				ctx.pages(); // This will throw if context is closed
				page = ctx.newPage();
				classLogger.debug("Reused existing browser context");
			} catch (Exception e) {
				// Context is closed/expired, need to create a new one
				classLogger.warn("Failed to create page from context (likely closed): {}, creating new context", e.getMessage());
				this.insight.getUser().setSharedPlaywrightContext(null);
				ctx = null;
			}
		}

		// Create new context if we don't have a valid one - synchronized to prevent race conditions
		if (ctx == null) {
			synchronized (CONTEXT_CREATION_LOCK) {
				// Double-check: another thread might have created a context while we were waiting
				ctx = this.insight.getUser().getSharedPlaywrightContext();

				if (ctx == null) {
					try {
						Browser.NewContextOptions ctxOps = new Browser.NewContextOptions().setViewportSize(width, height)
								.setDeviceScaleFactor(dpr);
						ctx = browser.newContext(ctxOps);
						ctx.setDefaultTimeout(60_000);
						ctx.setDefaultNavigationTimeout(60_000);
						this.insight.getUser().setSharedPlaywrightContext(ctx);
						classLogger.info("Created new shared browser context");
					} catch (Exception e) {
						classLogger.error("Failed to create browser context: {}", e.getMessage(), e);
						throw new RuntimeException("Failed to create Playwright session: " + e.getMessage(), e);
					}
				}

				// Create page from the context (either newly created or found during double-check)
				if (page == null) {
					page = ctx.newPage();
				}
			}
		}

		PlaywrightSession s = new PlaywrightSession(ctx, page);
		if (s.history.meta() == null) {
			s.history = new StepsEnvelope("1.0", PlaywrightSession.newMeta(""), s.history.steps());
		}

		String id = UUID.randomUUID().toString();
		s.setUserAndSessionId(this.insight.getUser(), id);
		this.insight.getUser().setPlaywrightSession(id, s);

		classLogger.info("Created playwright session successfully with id: {}", id);
		return new NounMetadata(id, PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return "Reactor that initiate a new session and return its id";
	}

}
