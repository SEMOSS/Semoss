package prerna.reactor.playwright;

import com.microsoft.playwright.Page;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import java.util.HashMap;
import java.util.Map;

public class DeleteTabReactor extends AbstractReactor {

    public DeleteTabReactor() {
        this.keysToGet = new String[] {
                "sessionId",
                "tabId"
        };
        this.keyRequired = new int[] { 1, 1 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String sessionId = this.keyValue.get(this.keysToGet[0]);
        String tabId = this.keyValue.get(this.keysToGet[1]);

        if (sessionId == null || sessionId.isEmpty()) {
            throw new IllegalArgumentException("sessionId is required");
        }
        if (tabId == null || tabId.isEmpty()) {
            throw new IllegalArgumentException("tabId is required");
        }

        Map<String, Object> result = deleteTab(sessionId, tabId);
        return new NounMetadata(result, PixelDataType.MAP);
    }

    private Map<String, Object> deleteTab(String sessionId, String tabId) {
        Map<String, Object> response = new HashMap<>();

        Session session = this.insight.getUser().getPlaywrightSession(sessionId);

        if (session == null) {
            throw new IllegalStateException("Session not found: " + sessionId);
        }

        if (!session.history.steps().containsKey(tabId)) {
            throw new IllegalArgumentException("Tab not found in session: " + tabId);
        }

        // Delete from in-memory session history
        session.history.steps().remove(tabId);

        // Remove the page from session without closing it immediately
        // Closing the page can cause session invalidation issues
        // The page will be cleaned up when the session is closed properly
        Page page = session.tabPages.remove(tabId);

        // Only close the page if there are other tabs remaining in the session
        // This prevents the session from being invalidated
        if (page != null && !page.isClosed() && session.tabPages.size() > 0) {
            try {
                page.close();
            } catch (Exception e) {
                // Ignore close errors
            }
        }

        session.tabCurrentPageIndex.remove(tabId);
        session.tabCurrentStepIndex.remove(tabId);

        response.put("success", true);
        response.put("message", "Tab " + tabId + " deleted from session. Changes will apply when SaveAllReactor is called.");
        response.put("deletedTab", tabId);

        return response;
    }
}

