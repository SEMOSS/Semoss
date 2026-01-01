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

	@Override
	public NounMetadata execute() {
		int width = 1280;
		int height = 800;
		double dpr = 1.0;

		Browser.NewContextOptions ctxOps = new Browser.NewContextOptions().setViewportSize(width, height)
			.setDeviceScaleFactor(dpr);

		SessionResources resources = prepareSessionResources(ctxOps);
		PlaywrightSession s = new PlaywrightSession(resources.context(), resources.page());
		if (s.history.meta() == null) {
			s.history = new StepsEnvelope("1.0", PlaywrightSession.newMeta(""), s.history.steps());
		}

		String id = UUID.randomUUID().toString();
		s.setUserAndSessionId(this.insight.getUser(), id);
		this.insight.getUser().setPlaywrightSession(id, s);

		classLogger.info("Created playwright session successfully with id: {}", id);
		return new NounMetadata(id, PixelDataType.CONST_STRING);
	}

	private SessionResources prepareSessionResources(Browser.NewContextOptions ctxOps) {
		BrowserContext ctx = acquireContextWithRetry(ctxOps);
		try {
			Page page = ctx.newPage();
			return new SessionResources(ctx, page);
		} catch (RuntimeException ex) {
			classLogger.warn("Failed to create page within existing Playwright context, retrying", ex);
			resetPlaywrightState();
			BrowserContext retryCtx = acquireContextWithRetry(ctxOps);
			try {
				Page retryPage = retryCtx.newPage();
				return new SessionResources(retryCtx, retryPage);
			} catch (RuntimeException retryEx) {
				throw new IllegalStateException("Unable to create Playwright page after retry", retryEx);
			}
		}
	}

	private BrowserContext acquireContextWithRetry(Browser.NewContextOptions ctxOps) {
		RuntimeException lastFailure = null;
		int numberOfAttempts = 3;
		for (int attempt = 1; attempt <= numberOfAttempts; attempt++) {
			try {
				Browser browser = PlaywrightBrowserProvider.getBrowser();
				BrowserContext ctx = this.insight.getUser().getOrCreateSharedPlaywrightContext(browser, ctxOps);
				if (ctx == null) {
					throw new IllegalStateException("Shared Playwright context is null");
				}
				return ctx;
			} catch (RuntimeException ex) {
				lastFailure = ex;
				classLogger.warn("Failed to initialize Playwright context (attempt {})", attempt, ex);
				resetPlaywrightState();
			}
		}
		throw new IllegalStateException(
				"Unable to initialize Playwright context after " + numberOfAttempts + " attempts.", lastFailure);
	}

	private void resetPlaywrightState() {
		try {
			this.insight.getUser().closeAndClearSharedPlaywrightContext();
		} catch (Exception ex) {
			classLogger.debug("Error while clearing shared Playwright context", ex);
		}
		PlaywrightBrowserProvider.shutdown();
	}

	private static final class SessionResources {
		private final BrowserContext context;
		private final Page page;

		private SessionResources(BrowserContext context, Page page) {
			this.context = context;
			this.page = page;
		}

		private BrowserContext context() {
			return context;
		}

		private Page page() {
			return page;
		}
	}

	@Override
	public String getReactorDescription() {
		return "Reactor that initiate a new session and return its id";
	}

}
