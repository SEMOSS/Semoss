package prerna.reactor.playwright;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetAllStepsReactor extends AbstractReactor {

	public GetAllStepsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), "sessionId", "fileName" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String sessionId = this.keyValue.get(this.keysToGet[1]);
		String fileName = this.keyValue.get(this.keysToGet[2]);

		if (sessionId == null || sessionId.isEmpty()) {
			throw new IllegalArgumentException("sessionId is required");
		}

		if (fileName == null || fileName.isEmpty()) {
			throw new IllegalArgumentException("fileName is required");
		}

		PlaywrightSession playwrightSession = this.insight.getUser().getPlaywrightSession(sessionId);
		if (playwrightSession == null) {
			throw new IllegalStateException("No active session found for sessionId: " + sessionId);
		}

		// Load steps from file
		StepsEnvelope env = PlaywrightUtility.loadStepsFromFile(projectId, fileName);
		if (env == null) {
			throw new IllegalStateException("Failed to load steps from file: " + fileName);
		}

		populateSessionHistory(playwrightSession, env);

		// Transform the steps from Map<String, List<List<Step>>> to the frontend format
		Map<String, List<Map<String, Object>>> transformedSteps = new HashMap<>();

		for (Map.Entry<String, List<List<PlaywrightStep>>> entry : env.steps().entrySet()) {
			String tabId = entry.getKey();
			List<List<PlaywrightStep>> pages = entry.getValue();

			List<Map<String, Object>> tabSteps = new ArrayList<>();
			for (List<PlaywrightStep> page : pages) {
				for (PlaywrightStep step : page) {
					Map<String, Object> stepMap = new HashMap<>();
					stepMap.put("id", step.id());
					stepMap.put("type", step.type().toString());

					if (step.shouldRun() != null) {
						stepMap.put("shouldRun", step.shouldRun());
					}

					if (step.tag() != null) {
						stepMap.put("tag", step.tag());
					}

					if (step.required() != null) {
						stepMap.put("required", step.required());
					}

					if (step.url() != null) {
						stepMap.put("url", step.url());
					}
					if (step.coords() != null) {
						Map<String, Object> coordsMap = new HashMap<>();
						coordsMap.put("x", step.coords().x());
						coordsMap.put("y", step.coords().y());
						stepMap.put("coords", coordsMap);
					}

					if (step.text() != null) {
						stepMap.put("text", step.text());
					}
					if (step.label() != null) {
						stepMap.put("label", step.label());
					}
					if (step.description() != null) {
						stepMap.put("description", step.description());
					}
					if (step.type() == PlaywrightStepType.TYPE) {
						stepMap.put("isPassword", step.isPassword());
						stepMap.put("storeValue", step.storeValue());

					}
					if (step.deltaY() != null) {
						stepMap.put("deltaY", step.deltaY());
					}
					if (step.type() == PlaywrightStepType.CONTEXT) {
						if (step.multiCoords() != null) {
							List<Map<String, Object>> multiCoordsList = new ArrayList<>();
							for (Coords coord : step.multiCoords()) {
								Map<String, Object> coordMap = new HashMap<>();
								coordMap.put("x", coord.x());
								coordMap.put("y", coord.y());
								multiCoordsList.add(coordMap);
							}
							stepMap.put("multiCoords", multiCoordsList);
						}
						if (step.prompt() != null) {
							stepMap.put("prompt", step.prompt());
						}
					}
					if (step.waitAfterMs() != null) {
						stepMap.put("waitAfterMs", step.waitAfterMs());
					}
					tabSteps.add(stepMap);
				}
			}
			transformedSteps.put(tabId, tabSteps);
		}

		Map<String, Object> response = new HashMap<>();
		response.put("steps", transformedSteps);
		response.put("success", true);
		response.put("sessionId", sessionId);
		return new NounMetadata(response, PixelDataType.MAP);
	}

	/**
	 * Populates the session history with steps from the loaded recording.
	 * This appends steps from the loaded file to the existing session history,
	 * allowing multiple files to be loaded sequentially into the same session.
	 * Step IDs are reassigned to ensure uniqueness across all loaded files.
	 *
	 * @param session The PlaywrightSession to populate
	 * @param env     The StepsEnvelope containing the loaded steps
	 */
	private void populateSessionHistory(PlaywrightSession session, StepsEnvelope env) {
		Map<String, List<List<PlaywrightStep>>> stepsMap = env.steps();

		int currentStepId = session.lastStepId;

		for (Map.Entry<String, List<List<PlaywrightStep>>> entry : stepsMap.entrySet()) {
			String tabId = entry.getKey();
			List<List<PlaywrightStep>> pages = entry.getValue();

			if (!session.history.steps().containsKey(tabId)) {
				session.history.steps().put(tabId, new ArrayList<>());
			}

			for (List<PlaywrightStep> page : pages) {
				List<PlaywrightStep> pageWithNewIds = new ArrayList<>();

				for (PlaywrightStep step : page) {
					currentStepId++;
					PlaywrightStep stepWithNewId = new PlaywrightStep(step, currentStepId);
					pageWithNewIds.add(stepWithNewId);
				}

				session.history.steps().get(tabId).add(pageWithNewIds);
			}
		}

		session.lastStepId = currentStepId;
		System.out.println("Loaded and appended steps from file. New lastStepId: " + currentStepId);
	}

	@Override
	public String getReactorDescription() {
		return "Retrieves all recorded Playwright steps from a specified file and formats them for frontend display.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id of the current session of the playwright";
		} else if (key.equals("fileName")) {
			return "The name of the record file";
		}

		return super.getDescriptionForKey(key);
	}
}
