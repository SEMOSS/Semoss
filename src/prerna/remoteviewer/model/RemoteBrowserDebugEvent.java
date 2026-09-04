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
package prerna.remoteviewer.model;

/**
 * Sanitized browser diagnostic metadata sent to the remote browser viewer.
 * Null fields are omitted by the socket serializer.
 */
public record RemoteBrowserDebugEvent(String id, String kind, String phase, String requestId, long timestamp,
		String tabId, String method, String url, String resourceType, Integer status, String statusText,
		Long durationMs, String error, String level, String message, String source) {

	public static RemoteBrowserDebugEvent network(String id, String phase, String requestId, long timestamp,
			String tabId, String method, String url, String resourceType, Integer status, String statusText,
			Long durationMs, String error) {
		return new RemoteBrowserDebugEvent(id, "network", phase, requestId, timestamp, tabId, method, url,
				resourceType, status, statusText, durationMs, error, null, null, null);
	}

	public static RemoteBrowserDebugEvent console(String id, long timestamp, String tabId, String level,
			String message, String source) {
		return new RemoteBrowserDebugEvent(id, "console", null, null, timestamp, tabId, null, null, null, null,
				null, null, null, level, message, source);
	}

	public static RemoteBrowserDebugEvent pageError(String id, long timestamp, String tabId, String message) {
		return new RemoteBrowserDebugEvent(id, "page-error", null, null, timestamp, tabId, null, null, null, null,
				null, null, null, "error", message, null);
	}
}
