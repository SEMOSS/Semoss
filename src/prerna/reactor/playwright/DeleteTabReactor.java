package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.playwright.Page;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteTabReactor extends AbstractReactor {

	public DeleteTabReactor() {
		this.keysToGet = new String[] { "sessionId", "tabId" };
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

		PlaywrightSession playwrightSession = this.insight.getUser().getPlaywrightSession(sessionId);
		if (playwrightSession == null) {
			throw new IllegalStateException("Session not found: " + sessionId);
		}

		if (!playwrightSession.history.steps().containsKey(tabId)) {
			throw new IllegalArgumentException("Tab not found in session: " + tabId);
		}

		// Check if the tab has children using the cached map
		List<String> children = playwrightSession.getChildTabs(tabId);
		if (!children.isEmpty()) {
			String joined = String.join(", ", children);
			int count = children.size();
			String plural = count == 1 ? "tab" : "tabs";
			throw new IllegalArgumentException("Cannot delete '" + tabId + "': " + count + " child " + plural
					+ " must be closed first (" + joined + ")");
		}

		playwrightSession.history.steps().remove(tabId);

		// Close the page
		Page page = playwrightSession.tabPages.remove(tabId);
		if (page != null && !page.isClosed() && playwrightSession.tabPages.size() > 0) {
			try {
				page.close();
			} catch (Exception e) {
				// ignore close errors
			}
		}

		playwrightSession.tabCurrentPageIndex.remove(tabId);
		playwrightSession.tabCurrentStepIndex.remove(tabId);

		playwrightSession.removeTabRelationships(tabId);

		Map<String, Object> response = new HashMap<>();
		response.put("success", true);
		response.put("message",
				"Tab " + tabId + " deleted from session. Changes will apply when SaveAllReactor is called.");
		response.put("deletedTab", tabId);
		return new NounMetadata(response, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Deletes a specific tab within a Playwright session, including its history and associated page.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the session to delete the playwright opened tab";
		} else if (key.equals("tabId")) {
			return "The id of the tab in the session that you want to delete";
		}

		return super.getDescriptionForKey(key);
	}
}
