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
package prerna.util.k8s;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * A Kubernetes API call that was answered with an error, or that could not be
 * made at all. {@link #getCode()} is the HTTP status (0 when the server was not
 * reached) and {@link #getReason()} the machine-readable reason of the API's
 * {@code Status} reply (NotFound, Forbidden, Conflict, Invalid, ...).
 */
public class KubeApiException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	private static final int MAX_RAW_MESSAGE = 500;

	private final int code;
	private final String reason;

	public KubeApiException(int code, String reason, String message) {
		super(message);
		this.code = code;
		this.reason = reason;
	}

	public KubeApiException(String message, Throwable cause) {
		super(message, cause);
		this.code = 0;
		this.reason = null;
	}

	/**
	 * Build the exception from an error reply. Kubernetes answers errors with a
	 * {@code Status} object; anything else (a proxy's HTML page, an empty body) is
	 * reported as is, shortened.
	 */
	static KubeApiException fromResponse(int code, String body) {
		String reason = null;
		String message = null;
		if (body != null && !body.isBlank()) {
			try {
				JsonElement parsed = JsonParser.parseString(body);
				if (parsed.isJsonObject()) {
					JsonObject status = parsed.getAsJsonObject();
					reason = KubeJson.str(status, "reason");
					message = KubeJson.str(status, "message");
				}
			} catch (RuntimeException e) {
				// not JSON: fall through to the raw body
			}
			if (message == null) {
				String raw = body.strip();
				message = raw.length() > MAX_RAW_MESSAGE ? raw.substring(0, MAX_RAW_MESSAGE) + "..." : raw;
			}
		}
		if (message == null || message.isEmpty()) {
			message = "HTTP " + code;
		}
		return new KubeApiException(code, reason, message);
	}

	/**
	 * @return the HTTP status, or 0 when the API server was not reached
	 */
	public int getCode() {
		return this.code;
	}

	/**
	 * @return the reason of the API's Status reply, or null
	 */
	public String getReason() {
		return this.reason;
	}

	public boolean isNotFound() {
		return this.code == 404;
	}

	public boolean isUnauthorized() {
		return this.code == 401 || this.code == 403;
	}

	public boolean isUnreachable() {
		return this.code == 0;
	}
}
