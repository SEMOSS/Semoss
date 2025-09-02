package prerna.reactor.playwright;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.options.WaitUntilState;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SessionReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(SessionReactor.class);
	private Browser browser;
    public final static Map<String, Session> sessions = new ConcurrentHashMap<>();

	public SessionReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
				};
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		Playwright pw = Playwright.create();
		browser = pw.chromium().launch(
	            new BrowserType.LaunchOptions().setHeadless(true));
	                    		
    	Map<String, Object> paramValues = getMap();

	    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		CreateSessionRequest createSessionRequest = json.convertValue(paramValues, CreateSessionRequest.class);
        return new NounMetadata(createAndOpen(createSessionRequest), PixelDataType.MAP);
	}
	
	public record CreateResult(String sessionId, ScreenshotResponse firstShot) {}
    public record CreateSessionRequest(String url, Integer width, Integer height, Double deviceScaleFactor) {}

	private CreateResult createAndOpen(CreateSessionRequest req) {
        int width  = req.width()  != null ? req.width()  : 1280;
        int height = req.height() != null ? req.height() : 800;
        double dpr = req.deviceScaleFactor() != null ? req.deviceScaleFactor() : 1.0;

        Browser.NewContextOptions ctxOps = new Browser.NewContextOptions()
                .setViewportSize(width, height)
                .setDeviceScaleFactor(dpr);

        BrowserContext ctx = browser.newContext(ctxOps);
        ctx.setDefaultTimeout(60_000);
        ctx.setDefaultNavigationTimeout(60_000);
        Page page = ctx.newPage();

        if (req.url() != null && !req.url().isBlank()) {
            page.navigate(req.url(), new Page.NavigateOptions().setWaitUntil(WaitUntilState.NETWORKIDLE));
        } else {
        	throw new IllegalArgumentException("URL is blank, please enter a valid url to start the session");
        }

        Session s = new Session(ctx, page);
        if (s.history.meta() == null) {
            s.history = new StepsEnvelope(
                    "1.0",
                    Session.newMeta(req.url()),    // seed title with URL (optional)
                    s.history.steps()
            );
        }
        String id = UUID.randomUUID().toString();
        sessions.put(id, s);

        // record NAVIGATE step if provided
        if (req.url() != null && !req.url().isBlank()) {
            s.history.steps().add(new Step(
                    StepType.NAVIGATE, req.url(), null, null, null, null, "networkidle", 0,
                    new Viewport(width, height, dpr), System.currentTimeMillis(), null
            ));
        }
        classLogger.info("Created playwright session successfully with id: {}", id);
        return new CreateResult(id, ScreenshotReactor.screenshot(id));
    }
	
    public static Session get(String id) {
        Session s = sessions.get(id);
        if (s == null) throw new RuntimeException("Unknown session: " + id);
        return s;
    }
	
	@SuppressWarnings("unchecked")
	public Map<String, Object> getMap() {
        GenRowStruct mapGrs = store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
        if(mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if(mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if(mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return null;
    }

}
