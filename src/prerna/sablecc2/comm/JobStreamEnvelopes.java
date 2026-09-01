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
package prerna.sablecc2.comm;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Generic stream envelopes that apply to any Pixel async job, agent or not. */
public final class JobStreamEnvelopes {

	private JobStreamEnvelopes() {}

	/** Terminal {@code job-cancelled} envelope for any jobId. No-op if jobId is null/blank. */
	public static void jobCancelled(String jobId, String reason) {
		if (jobId == null || jobId.isBlank()) return;
		Map<String, Object> data = new LinkedHashMap<>();
		data.put("kind", "job-cancelled");
		data.put("reason", reason != null && !reason.isBlank() ? reason : "user-requested");
		data.put("timestamp", Instant.now().toString());
		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("stream_type", "content");
		envelope.put("data", data);
		PixelJobManager.getManager().addStreamOut(jobId, envelope);
	}

	/** Emits a room-name update through an existing Pixel async job stream. */
	public static void roomName(String jobId, String roomName) {
		if (jobId == null || jobId.isBlank() || roomName == null || roomName.isBlank()) return;
		Map<String, Object> data = new LinkedHashMap<>();
		data.put("roomName", roomName);
		Map<String, Object> envelope = new LinkedHashMap<>();
		envelope.put("stream_type", "room_name");
		envelope.put("data", data);
		PixelJobManager.getManager().addStreamOut(jobId, envelope);
	}
}
