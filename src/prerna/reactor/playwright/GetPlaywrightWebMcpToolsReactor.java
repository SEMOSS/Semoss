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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 * Licensed under the Apache License, Version 2.0 (the "License");
 *******************************************************************************/
package prerna.reactor.playwright;

import java.util.LinkedHashMap;
import java.util.Map;

import com.microsoft.playwright.Page;

import prerna.reactor.AbstractReactor;
import prerna.remoteviewer.service.RemoteBrowserSession;
import prerna.remoteviewer.service.RemoteBrowserSessionManager;
import prerna.remoteviewer.service.RemoteBrowserWebMcpService;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Lists WebMCP tools exposed by the active page in an owned browser session. */
public class GetPlaywrightWebMcpToolsReactor extends AbstractReactor {

	private static final String KEY_SESSION_ID = "sessionId";

	public GetPlaywrightWebMcpToolsReactor() {
		this.keysToGet = new String[] { KEY_SESSION_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Map<String, Object> result = new LinkedHashMap<>();
		try {
			RemoteBrowserSession session = ownedSession(clean(this.keyValue.get(KEY_SESSION_ID)));
			session.getPlaywrightSession().getOperationLock().lock();
			try {
				Page page = session.getActivePage();
				result.put("success", true);
				result.putAll(RemoteBrowserWebMcpService.discover(page));
				result.put("pageUrl", page.url());
				result.put("tabId", session.getActiveTabId());
			} finally {
				session.getPlaywrightSession().getOperationLock().unlock();
			}
		} catch (Exception e) {
			result.put("success", false);
			result.put("supported", false);
			result.put("error", e.getMessage() == null ? "WebMCP tool discovery failed" : e.getMessage());
		}
		return new NounMetadata(result, PixelDataType.MAP);
	}

	private RemoteBrowserSession ownedSession(String sessionId) {
		RemoteBrowserSession session = RemoteBrowserSessionManager.getInstance().getSession(sessionId).orElse(null);
		if (session == null) {
			throw new IllegalArgumentException("Browser session '" + sessionId + "' not found");
		}
		String userId = this.insight.getUser().getPrimaryLoginToken().getId();
		if (!userId.equals(session.getUserId())) {
			throw new IllegalArgumentException("Browser session does not belong to the current user");
		}
		Page page = session.getActivePage();
		if (page == null || page.isClosed()) {
			throw new IllegalArgumentException("No active browser page");
		}
		return session;
	}

	private static String clean(Object value) {
		return value == null ? "" : String.valueOf(value).trim();
	}

	@Override
	public String getReactorDescription() {
		return "Lists WebMCP tools exposed by the active remote browser page.";
	}
}
