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
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CheckNetworkIdleReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CheckNetworkIdleReactor.class);

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
			} catch (NumberFormatException e) {
				classLogger.warn("Invalid quietMillis value '{}'; using default of {} ms", quietRaw, DEFAULT_QUIET_MS);
			}
		}

		PlaywrightSession playwrightSession = this.insight.getUser().getPlaywrightSession(sessionId);
		if (playwrightSession == null) {
			throw new IllegalArgumentException("No playwright session found for id: " + sessionId);
		}

		playwrightSession.refreshTrackedUrl(tabId);
		boolean isIdle = playwrightSession.isNetworkIdle(tabId, quietMillis);

		Map<String, Object> response = new HashMap<>();
		response.put("isNetworkIdle", isIdle);
		response.put("inFlightRequests", playwrightSession.getInFlightRequests(tabId));
		response.put("lastActivityTs", playwrightSession.getLastNetworkActivity(tabId));
		response.put("quietMillis", quietMillis);
		response.put("currentUrl", playwrightSession.getCurrentUrl(tabId));

		return new NounMetadata(response, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Checks if the network activity for a specific Playwright session tab is idle.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("sessionId")) {
			return "The id for the current session";
		} else if (key.equals("tabId")) {
			return "the tab id for the current session ";
		}
		return super.getDescriptionForKey(key);
	}
}
