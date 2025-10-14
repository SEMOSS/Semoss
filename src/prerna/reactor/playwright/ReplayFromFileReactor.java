package prerna.reactor.playwright;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;

import prerna.om.Insight;
import prerna.reactor.AbstractReactor;
import prerna.reactor.playwright.PlaywrightUtility;
import prerna.reactor.playwright.SessionUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;


public class ReplayFromFileReactor extends AbstractReactor {
	
	public static Path recordingsDir = PlaywrightUtility.initRecordingsDir();
    static ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    static Insight insightObj;
	Browser browser;
	
	public ReplayFromFileReactor(){
		this.keysToGet = new String[] {
				"sessionId",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
				};
		this.keyRequired = new int[] { 1,1 };
		insightObj = this.insight;
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
    	Map<String, Object> paramValues = Utility.getMap(this.store, this.curRow);

        return new NounMetadata(replayFromFile(paramValues.get("name").toString()), PixelDataType.MAP);
	}
	
    public ScreenshotResponse replayFromFile(String nameOrPath) {
        StepsEnvelope env = PlaywrightUtility.loadStepsFromFile(nameOrPath);
        return replay(env);
    }
	
	public ScreenshotResponse replay(StepsEnvelope steps) {
		Playwright pw = Playwright.create();
		browser = pw.chromium().launch(
	            new BrowserType.LaunchOptions().setHeadless(true));
        BrowserContext ctx = browser.newContext(new Browser.NewContextOptions()
                .setViewportSize(steps.steps().get(0).get(0).viewport().width(),
                        steps.steps().get(0).get(0).viewport().height())
                .setDeviceScaleFactor(steps.steps().get(0).get(0).viewport().deviceScaleFactor())
        );
        Page page = ctx.newPage();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
        Session s = SessionReactor.get(sessionId);

        for (int i = 0; i<steps.steps().size();i++) {
        	for (Step st : steps.steps().get(i)) {
            	SessionUtility.applyStep(s, st);
        	}
        } 
        s.history = steps;
        return ScreenshotReactor.screenshot(sessionId);
    }
	
    public List<String> listRecordings() {
        try (var stream = Files.list(recordingsDir)) {
            return stream.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .map(p -> p.getFileName().toString())
                    .sorted()
                    .toList();
        } catch (Exception e) {
            throw new RuntimeException("Failed to list recordings", e);
        }
    }
}
