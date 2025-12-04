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
		Browser browser = PlaywrightBrowserProvider.getBrowser();

		int width = 1280;
		int height = 800;
		double dpr = 1.0;

		BrowserContext ctx = this.insight.getUser().getSharedPlaywrightContext();
		if (ctx == null) {
			Browser.NewContextOptions ctxOps = new Browser.NewContextOptions().setViewportSize(width, height)
					.setDeviceScaleFactor(dpr);
			ctx = browser.newContext(ctxOps);
			ctx.setDefaultTimeout(60_000);
			ctx.setDefaultNavigationTimeout(60_000);
			this.insight.getUser().setSharedPlaywrightContext(ctx);
		}
		Page page = ctx.newPage();

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
