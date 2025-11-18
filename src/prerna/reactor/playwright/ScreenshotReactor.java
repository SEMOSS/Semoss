package prerna.reactor.playwright;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.options.LoadState;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class ScreenshotReactor extends AbstractReactor {

    ObjectMapper json = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final Logger classLogger = LogManager.getLogger(ScreenshotReactor.class);

    public ScreenshotReactor() {
        this.keysToGet = new String[]{
                "sessionId",
                "tabId",
                ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[]{1, 1, 0};  // extra parameters optional
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String sessionId = this.keyValue.get(this.keysToGet[0]);
        Session session = this.insight.getUser().getPlaywrightSession(sessionId);
        String tabId = this.keyValue.get(this.keysToGet[1]);

        // check if crop params are provided
        Map<String, Object> paramValues = getMap(this.keysToGet[2]);

        if (paramValues != null && paramValues.containsKey("startX")) {
            //log the crop params
            classLogger.info("Crop params provided.");
            classLogger.info("Crop params: " + paramValues.toString());
            // cropped screenshot
            int startX = ((Number) paramValues.get("startX")).intValue();
            int startY = ((Number) paramValues.get("startY")).intValue();
            int endX = ((Number) paramValues.get("endX")).intValue();
            int endY = ((Number) paramValues.get("endY")).intValue();

            return new NounMetadata(croppedScreenshot(session, tabId, startX, startY, endX, endY), PixelDataType.MAP);
        } else {
            // normal screenshot
            return new NounMetadata(screenshot(session, tabId), PixelDataType.MAP);
        }
    }

    public static ScreenshotResponse screenshot(Session s, String tabId) {
        Page page = s.tabPages.get(tabId);
        waitForStablePage(page);
        s.refreshTrackedUrl(tabId);
        byte[] buf = page.screenshot(new Page.ScreenshotOptions().setFullPage(false));
        String b64 = java.util.Base64.getEncoder().encodeToString(buf);

        int vpW = page.viewportSize().width;
        int vpH = page.viewportSize().height;

        Object raw = page.evaluate("() => Number.isFinite(window.devicePixelRatio) ? window.devicePixelRatio : 1");
        double dpr = (raw instanceof Number) ? ((Number) raw).doubleValue() : 1.0;

        return new ScreenshotResponse(b64, vpW, vpH, dpr);
    }

    public static ScreenshotResponse croppedScreenshot(Session s, String tabId, int startX, int startY, int endX, int endY) {
        Page page = s.tabPages.get(tabId);
        waitForStablePage(page);
        s.refreshTrackedUrl(tabId);

        int x = Math.min(startX, endX);
        int y = Math.min(startY, endY);
        int width = Math.abs(endX - startX);
        int height = Math.abs(endY - startY);

        byte[] buf = page.screenshot(new Page.ScreenshotOptions()
                .setFullPage(false)
                .setClip(x, y, width, height));

        String b64 = java.util.Base64.getEncoder().encodeToString(buf);

        return new ScreenshotResponse(b64, width, height, 1.0);
    }

    private static void waitForStablePage(Page page) {
        try {
            page.waitForLoadState(LoadState.NETWORKIDLE,
                    new Page.WaitForLoadStateOptions().setTimeout(5_000));
        } catch (Exception e) {
            try {
                page.waitForLoadState(LoadState.LOAD,
                        new Page.WaitForLoadStateOptions().setTimeout(2_000));
            } catch (Exception ignored) {
                // give up and fall back to immediate screenshot
            }
        }
    }

    @Override
    public String getReactorDescription() {
        return "Reactor that that return a fresh screenshot for a session";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals("sessionId")) {
            return "The id of the current session of the playwright";
        } else if (key.equals("tabId")) {
            return "The id of the current tab of the playwright";
        }
        return super.getDescriptionForKey(key);
    }

}
