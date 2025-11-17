package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CheckNetworkIdleReactor extends AbstractReactor {

    private static final long DEFAULT_QUIET_MS = 500;

    public CheckNetworkIdleReactor() {
        this.keysToGet = new String[] { "sessionId", "tabId", "quietMillis" };
        this.keyRequired = new int[] { 1, 1, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String sessionId = this.keyValue.get(this.keysToGet[0]);
        String tabId = this.keyValue.get(this.keysToGet[1]);
        String quietRaw = this.keyValue.get(this.keysToGet[2]);

        if (sessionId == null || tabId == null) {
            throw new IllegalArgumentException("sessionId and tabId are required");
        }

        long quietMillis = DEFAULT_QUIET_MS;
        if (quietRaw != null) {
            try {
                quietMillis = Long.parseLong(quietRaw);
            } catch (NumberFormatException ignore) {
                // use default
            }
        }

        Session session = this.insight.getUser().getPlaywrightSession(sessionId);
        if (session == null) {
            throw new IllegalArgumentException("No playwright session found for id: " + sessionId);
        }

        session.refreshTrackedUrl(tabId);
        boolean isIdle = session.isNetworkIdle(tabId, quietMillis);

        Map<String, Object> response = new HashMap<>();
        response.put("isNetworkIdle", isIdle);
        response.put("inFlightRequests", session.getInFlightRequests(tabId));
        response.put("lastActivityTs", session.getLastNetworkActivity(tabId));
        response.put("quietMillis", quietMillis);
        response.put("currentUrl", session.getCurrentUrl(tabId));

        return new NounMetadata(response, PixelDataType.MAP);
    }
}
