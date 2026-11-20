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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Deletes a step from the Playwright session history. This reactor removes a
 * specific step identified by its ID from a given tab within a Playwright
 * session. After deletion, it captures a new screenshot of the page to reflect
 * the state change. The keys to get are "sessionId", "tabId", and "stepId".
 */
public class DeleteStepReactor extends AbstractReactor {

	/**
	 * Constructor for the DeleteStepReactor. Initializes the required keys:
	 * "sessionId", "tabId", and "stepId".
	 */
	public DeleteStepReactor() {
		this.keysToGet = new String[] { "sessionId", "tabId", "stepId" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	/**
	 * Executes the deletion of a step from the Playwright session. It retrieves the
	 * session and tab IDs, finds the specified step, and removes it from the
	 * history. A new screenshot is taken after the deletion.
	 *
	 * @return A NounMetadata object containing the success status, the ID of the
	 *         deleted step, the new screenshot, and a message indicating that
	 *         changes need to be saved.
	 */
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

	/**
	 * Deletes a step from the history of a specific tab in a Playwright session.
	 *
	 * @param session The Playwright session from which to delete the step.
	 * @param tabId   The ID of the tab containing the step.
	 * @param stepId  The ID of the step to be deleted.
	 * @return {@code true} if the step was successfully deleted, {@code false}
	 *         otherwise.
	 */
	private boolean deleteStepFromHistory(PlaywrightSession session, String tabId, int stepId) {
		List<List<PlaywrightStep>> pages = session.history.steps().get(tabId);
		if (pages == null) {
			return false;
		}

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
