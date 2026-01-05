package prerna.reactor.playwright;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteStepReactor extends AbstractReactor {

	public DeleteStepReactor() {
		this.keysToGet = new String[] { "sessionId", "tabId", "stepId" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		String tabId = this.keyValue.get(this.keysToGet[1]);
		int stepId = Integer.parseInt(this.keyValue.get(this.keysToGet[2]));

		PlaywrightSession session = this.insight.getUser().getPlaywrightSession(sessionId);
		if (session == null) {
			throw new IllegalStateException("Session not found: " + sessionId);
		}

		boolean deleted = deleteStepFromHistory(session, tabId, stepId);

		if (!deleted) {
			throw new IllegalArgumentException("Step with ID " + stepId + " not found in tab " + tabId);
		}

		// Capture screenshot after deletion
		ScreenshotResponse screenshot = ScreenshotReactor.screenshot(session, tabId);

		Map<String, Object> response = new HashMap<>();
		response.put("success", true);
		response.put("deletedStepId", stepId);
		response.put("screenshot", screenshot);
		response.put("message", "Step deleted. Call SaveAllReactor to persist changes.");

		return new NounMetadata(response, PixelDataType.MAP);
	}

	private boolean deleteStepFromHistory(PlaywrightSession session, String tabId, int stepId) {
		List<List<PlaywrightStep>> pages = session.history.steps().get(tabId);
		if (pages == null)
			return false;

		for (List<PlaywrightStep> page : pages) {
			boolean removed = page.removeIf(step -> step.id() == stepId);
			if (removed) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String getReactorDescription() {
		return "Deletes a specific step from the Playwright session history by step ID.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The ID of the current Playwright session";
		} else if (key.equals("tabId")) {
			return "The ID of the tab containing the step";
		} else if (key.equals("stepId")) {
			return "The ID of the step to delete";
		}
		return super.getDescriptionForKey(key);
	}
}
