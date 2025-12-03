package prerna.reactor.playwright;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateStepReactor extends AbstractReactor {

	/**
	 * Represents the result of an update operation, containing a screenshot and the
	 * list of updated steps.
	 *
	 * @param screenshot   The {@link ScreenshotResponse} captured after the update.
	 * @param updatedSteps A list of {@link PlaywrightStep}s that were updated.
	 */
	private record UpdateResult(ScreenshotResponse screenshot, List<PlaywrightStep> updatedSteps) {
	}

	/**
	 * Default constructor for UpdateStepReactor. Initializes the keys this reactor
	 * expects: sessionId, tabId, and inputs.
	 */
	public UpdateStepReactor() {
		this.keysToGet = new String[] { "sessionId", "tabId", "inputs" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	/**
	 * Executes the reactor to update one or more Playwright steps in the current
	 * session's history.
	 *
	 * @return A NounMetadata object containing a map with the screenshot after the
	 *         update and a list of the updated steps.
	 * @throws IllegalArgumentException If the session or a specified step is not
	 *                                  found.
	 */
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String sessionId = this.keyValue.get(this.keysToGet[0]);
		String tabId = this.keyValue.get(this.keysToGet[1]);

		GenRowStruct inputs = this.store.getGenRowStruct("inputs");
		List<Object> steps = inputs.getAllValues();
		ObjectMapper mapper = new ObjectMapper();
		List<PlaywrightStep> stepList = mapper.convertValue(steps, new TypeReference<List<PlaywrightStep>>() {
		});

		UpdateResult result = updateStep(sessionId, tabId, stepList);

		HashMap<String, Object> response = new HashMap<>();
		response.put("screenshot", result.screenshot);
		response.put("updatedSteps", result.updatedSteps);

		return new NounMetadata(response, PixelDataType.MAP);
	}

	/**
	 * Updates one or more {@link PlaywrightStep}s in the session's history for a
	 * specific tab.
	 *
	 * @param sessionId The ID of the current Playwright session.
	 * @param tabId     The ID of the tab whose steps are to be updated.
	 * @param inputs    A list of {@link PlaywrightStep} objects containing the
	 *                  updates. Each step in this list must have an ID matching an
	 *                  existing step.
	 * @return An {@link UpdateResult} containing a screenshot after the update and
	 *         the list of steps that were updated.
	 * @throws IllegalArgumentException If a step with the given ID is not found in
	 *                                  the session history.
	 */
	private UpdateResult updateStep(String sessionId, String tabId, List<PlaywrightStep> inputs) {
		PlaywrightSession session = this.insight.getUser().getPlaywrightSession(sessionId);
		List<PlaywrightStep> updatedSteps = new ArrayList<>();

		List<OrderChange> orderChanges = new ArrayList<>();

		for (PlaywrightStep step : inputs) {
			session.history.steps().get(tabId).stream()
					.flatMap(outer -> IntStream.range(0, outer.size()).mapToObj(i -> new Object[] { outer, i }))
					.filter(a -> ((List<PlaywrightStep>) a[0]).get((int) a[1]).id() == step.id()).findFirst()
					.ifPresentOrElse(a -> {
						@SuppressWarnings("unchecked")
						List<PlaywrightStep> list = (List<PlaywrightStep>) a[0];
						int index = (int) a[1];
						PlaywrightStep existingStep = list.get(index);

						// check if order has changed
						if (existingStep.order() != step.order()) {
							orderChanges.add(new OrderChange(step.id(), existingStep.order(), step.order()));
						}
					}, () -> {
						throw new IllegalArgumentException("Step with ID " + step.id() + " not found.");
					});
		}

		if (!orderChanges.isEmpty()) {
			reorderSteps(session, tabId, orderChanges);
		}

		for (PlaywrightStep step : inputs) {
			// Find the step to update within the session history
			session.history.steps().get(tabId).stream()
					// Pair list with index
					.flatMap(outer -> IntStream.range(0, outer.size()).mapToObj(i -> new Object[] { outer, i }))
					.filter(a -> ((List<PlaywrightStep>) a[0]).get((int) a[1]).id() == step.id()).findFirst()
					.ifPresentOrElse(a -> {
						@SuppressWarnings("unchecked")
						List<PlaywrightStep> list = (List<PlaywrightStep>) a[0];
						int index = (int) a[1];
						PlaywrightStep existingStep = list.get(index);
						PlaywrightStep updatedStep = updateStep(existingStep, step);
						list.set(index, updatedStep); // Update the step in place
						updatedSteps.add(updatedStep);
					}, () -> {
						throw new IllegalArgumentException("Step with ID " + step.id() + " not found.");
					});
		}

		ScreenshotResponse screenshot = ScreenshotReactor.screenshot(session, tabId);
		return new UpdateResult(screenshot, updatedSteps);
	}

	/**
	 * Record to hold information about an order change for a step.
	 *
	 * @param stepId   The ID of the step being moved.
	 * @param oldOrder The original order of the step.
	 * @param newOrder The new order for the step.
	 */
	private record OrderChange(int stepId, int oldOrder, int newOrder) {
	}

	/**
	 * Reorders all steps in a tab based on the order changes requested.
	 * When a step moves from one position to another, all affected steps
	 * have their orders adjusted accordingly.
	 *
	 * @param session      The Playwright session.
	 * @param tabId        The ID of the tab whose steps are to be reordered.
	 * @param orderChanges A list of {@link OrderChange} objects describing the order changes.
	 */
	private void reorderSteps(PlaywrightSession session, String tabId, List<OrderChange> orderChanges) {
		List<List<PlaywrightStep>> stepsHistory = session.history.steps().get(tabId);
		List<PlaywrightStep> allSteps = new ArrayList<>();
		List<Integer> listIndices = new ArrayList<>();

		for (int i = 0; i < stepsHistory.size(); i++) {
			List<PlaywrightStep> innerList = stepsHistory.get(i);
			for (int j = 0; j < innerList.size(); j++) {
				allSteps.add(innerList.get(j));
				listIndices.add(i);
			}
		}

		for (OrderChange change : orderChanges) {
			int oldOrder = change.oldOrder();
			int newOrder = change.newOrder();

			PlaywrightStep movingStep = null;
			int movingStepIndex = -1;
			for (int i = 0; i < allSteps.size(); i++) {
				if (allSteps.get(i).id() == change.stepId()) {
					movingStep = allSteps.get(i);
					movingStepIndex = i;
					break;
				}
			}

			if (movingStep == null) {
				continue;
			}

			// re-order of all affected steps
			if (oldOrder < newOrder) {
				// Moving down: shift steps between oldOrder+1 and newOrder up by 1
				for (int i = 0; i < allSteps.size(); i++) {
					PlaywrightStep step = allSteps.get(i);
					if (step.order() > oldOrder && step.order() <= newOrder) {
						PlaywrightStep updatedStep = new PlaywrightStep(
							step.id(), step.order() - 1, step.type(), step.url(), step.coords(),
							step.multiCoords(), step.prompt(), step.text(), step.pressEnter(),
							step.deltaY(), step.waitUntil(), step.waitAfterMs(), step.viewport(),
							step.timestamp(), step.label(), step.description(), step.isPassword(),
							step.storeValue(), step.selector(), step.isTriggerNewTab(),
							step.shouldRun(), step.required()
						);
						allSteps.set(i, updatedStep);
					}
				}
			} else if (oldOrder > newOrder) {
				// Moving up: shift steps between newOrder and oldOrder-1 down by 1
				for (int i = 0; i < allSteps.size(); i++) {
					PlaywrightStep step = allSteps.get(i);
					if (step.order() >= newOrder && step.order() < oldOrder) {
						PlaywrightStep updatedStep = new PlaywrightStep(
							step.id(), step.order() + 1, step.type(), step.url(), step.coords(),
							step.multiCoords(), step.prompt(), step.text(), step.pressEnter(),
							step.deltaY(), step.waitUntil(), step.waitAfterMs(), step.viewport(),
							step.timestamp(), step.label(), step.description(), step.isPassword(),
							step.storeValue(), step.selector(), step.isTriggerNewTab(),
							step.shouldRun(), step.required()
						);
						allSteps.set(i, updatedStep);
					}
				}
			}

			PlaywrightStep updatedMovingStep = new PlaywrightStep(
				movingStep.id(), newOrder, movingStep.type(), movingStep.url(),
				movingStep.coords(), movingStep.multiCoords(), movingStep.prompt(),
				movingStep.text(), movingStep.pressEnter(), movingStep.deltaY(),
				movingStep.waitUntil(), movingStep.waitAfterMs(), movingStep.viewport(),
				movingStep.timestamp(), movingStep.label(), movingStep.description(),
				movingStep.isPassword(), movingStep.storeValue(), movingStep.selector(),
				movingStep.isTriggerNewTab(), movingStep.shouldRun(), movingStep.required()
			);
			allSteps.set(movingStepIndex, updatedMovingStep);
		}

		// Put the reordered steps back into the original structure
		int flatIndex = 0;
		for (int i = 0; i < stepsHistory.size(); i++) {
			List<PlaywrightStep> innerList = stepsHistory.get(i);
			for (int j = 0; j < innerList.size(); j++) {
				if (listIndices.get(flatIndex) == i) {
					innerList.set(j, allSteps.get(flatIndex));
					flatIndex++;
				}
			}
		}
	}

	/**
	 * Creates a new {@link PlaywrightStep} by applying updates from an input step
	 * to an existing step. This method handles specific logic for password fields
	 * (masking text). Note: The order is preserved from the existing step as
	 * order changes are handled separately in reorderSteps().
	 *
	 * @param existing The existing {@link PlaywrightStep}.
	 * @param input    The {@link PlaywrightStep} containing the updated values.
	 * @return A new {@link PlaywrightStep} with the applied updates.
	 */
	private PlaywrightStep updateStep(PlaywrightStep existing, PlaywrightStep input) {
		String label = input.label() != null ? input.label() : existing.label();
		String text = input.text() != null ? input.text() : existing.text();
		String description = input.description() != null ? input.description() : existing.description();
		Boolean shouldRun = input.shouldRun() != null ? input.shouldRun() : existing.shouldRun();
		Boolean required = input.required() != null ? input.required() : existing.required();
		boolean storeValue = input.storeValue(); // primitive boolean, always has a value

		// Use the existing order as it may have been updated in reorderSteps()
		int order = existing.order();

		if (existing.isPassword()) {
			// For password fields, the text is always masked when updating
			return new PlaywrightStep(
				existing.id(), order, existing.type(), existing.url(), existing.coords(),
				existing.multiCoords(), existing.prompt(), "", existing.pressEnter(),
				existing.deltaY(), existing.waitUntil(), existing.waitAfterMs(),
				existing.viewport(), existing.timestamp(), label, description,
				existing.isPassword(), false, existing.selector(), existing.isTriggerNewTab(),
				shouldRun != null ? shouldRun : false, required != null ? required : false
			);
		} else {
			return new PlaywrightStep(
				existing.id(), order, existing.type(), existing.url(), existing.coords(),
				existing.multiCoords(), existing.prompt(), text, existing.pressEnter(),
				existing.deltaY(), existing.waitUntil(), existing.waitAfterMs(),
				existing.viewport(), existing.timestamp(), label, description,
				existing.isPassword(), storeValue, existing.selector(), existing.isTriggerNewTab(),
				shouldRun != null ? shouldRun : false, required != null ? required : false
			);
		}
	}

	/**
	 * Returns a description of this reactor.
	 * 
	 * @return A string describing the reactor's function.
	 */
	@Override
	public String getReactorDescription() {
		return "Updates one or more Playwright steps in the current session's history.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The session id of the current playwright session";
		} else if (key.equals("tabId")) {
			return "The tab id of the current playwright session";
		} else if (key.equals("inputs")) {
			return "A list of PlaywrightStep objects containing the updates for existing steps.";
		}

		return super.getDescriptionForKey(key);
	}
}
