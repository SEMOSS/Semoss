/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.playwright;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import com.google.gson.reflect.TypeToken;

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
		List<PlaywrightStep> stepList = GSON.fromJson(GSON.toJson(steps), new TypeToken<List<PlaywrightStep>>() {
		}.getType());

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
	 * Creates a new {@link PlaywrightStep} by applying updates from an input step
	 * to an existing step. This method handles specific logic for password fields
	 * (masking text).
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

		if (existing.isPassword()) {
			// For password fields, the text is always masked when updating
			return new PlaywrightStep(existing, label, "", false, description, shouldRun != null ? shouldRun : false,
					required != null ? required : false);
		} else {
			return new PlaywrightStep(existing, label, text, storeValue, description,
					shouldRun != null ? shouldRun : false, required != null ? required : false);
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
