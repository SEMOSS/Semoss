package prerna.reactor.playwright;

import com.microsoft.playwright.Page;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeleteTabReactor extends AbstractReactor {

    public DeleteTabReactor() {
        this.keysToGet = new String[]{
                "sessionId",
                "tabId"
        };
        this.keyRequired = new int[]{1, 1};
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

        // Check if the tab has children using the cached map
        List<String> children = session.getChildTabs(tabId);
        if (!children.isEmpty()) {
            String joined = String.join(", ", children);
            int count = children.size();
            String plural = count == 1 ? "tab" : "tabs";
            throw new IllegalArgumentException(
                    "Cannot delete '" + tabId + "': " + count + " child " + plural + " must be closed first (" + joined + ")"
            );
        }

        session.history.steps().remove(tabId);

        // Close the page
        Page page = session.tabPages.remove(tabId);
        if (page != null && !page.isClosed() && session.tabPages.size() > 0) {
            try {
                page.close();
            } catch (Exception e) {
                // ignore close errors
            }
        }

        session.tabCurrentPageIndex.remove(tabId);
        session.tabCurrentStepIndex.remove(tabId);

        session.removeTabRelationships(tabId);

        response.put("success", true);
        response.put("message", "Tab " + tabId + " deleted from session. Changes will apply when SaveAllReactor is called.");
        response.put("deletedTab", tabId);

        return response;
    }

    @Override
    public String getReactorDescription() {
        return "Reactor to delete the playwright opened tab";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals("sessionId")) {
            return "The id of the session to delete the playwright opened tab";
        } else if (key.equals("tabId")) {
            return "The id of the tab in the session that you want to delete";
        }else {
            return super.getDescriptionForKey(key);
        }
    }
}

