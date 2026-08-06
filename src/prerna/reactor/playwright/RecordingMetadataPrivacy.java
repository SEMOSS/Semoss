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

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/** Privacy helpers used only for the recording-metadata model request. */
final class RecordingMetadataPrivacy {

	static final String REDACTED = "[REDACTED]";

	private static final Pattern SENSITIVE_ASSIGNMENT = Pattern.compile(
			"(?i)\\b(password|passcode|secret|token|api[ _-]?key|authorization|auth[ _-]?code|verification[ _-]?code|otp|email|phone|ssn|social[ _-]?security|credit[ _-]?card|card[ _-]?number)\\b\\s*(?:is|:|=)\\s*([^\\s,;]+)");
	private static final Pattern EMAIL = Pattern
			.compile("(?i)\\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}\\b");
	private static final Pattern BEARER = Pattern.compile("(?i)\\bBearer\\s+[A-Za-z0-9._~+/=-]+");
	private static final Pattern JWT = Pattern
			.compile("\\beyJ[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}\\b");
	private static final Pattern UUID = Pattern.compile(
			"(?i)\\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\\b");
	private static final Pattern CARD_OR_LONG_NUMBER = Pattern.compile("(?<![A-Za-z0-9])(?:\\d[ -]?){9,19}(?![A-Za-z0-9])");
	private static final Pattern PHONE = Pattern
			.compile("(?<![A-Za-z0-9])(?:\\+?\\d{1,3}[ .-]?)?(?:\\(?\\d{2,4}\\)?[ .-]?)?\\d{3,4}[ .-]\\d{3,4}(?![A-Za-z0-9])");
	private static final Pattern LONG_SECRET = Pattern.compile("\\b[A-Za-z0-9_+/=-]{24,}\\b");
	private static final Pattern SPACE = Pattern.compile("\\s+");

	private RecordingMetadataPrivacy() {
	}

	static String sanitizeText(String value, int maxLength) {
		if (value == null || value.isBlank()) {
			return "";
		}
		String sanitized = SPACE.matcher(value).replaceAll(" ").trim();
		sanitized = SENSITIVE_ASSIGNMENT.matcher(sanitized).replaceAll("$1=" + REDACTED);
		sanitized = BEARER.matcher(sanitized).replaceAll("Bearer " + REDACTED);
		sanitized = JWT.matcher(sanitized).replaceAll(REDACTED);
		sanitized = EMAIL.matcher(sanitized).replaceAll(REDACTED);
		sanitized = UUID.matcher(sanitized).replaceAll(REDACTED);
		sanitized = CARD_OR_LONG_NUMBER.matcher(sanitized).replaceAll(REDACTED);
		sanitized = PHONE.matcher(sanitized).replaceAll(REDACTED);
		sanitized = LONG_SECRET.matcher(sanitized).replaceAll(REDACTED);
		if (maxLength > 0 && sanitized.length() > maxLength) {
			return sanitized.substring(0, maxLength).trim() + "...";
		}
		return sanitized;
	}

	/**
	 * Keeps only origin and a scrubbed path. Query, fragment and user-info are never
	 * included in the model prompt.
	 */
	static String sanitizeUrl(String value) {
		if (value == null || value.isBlank()) {
			return "";
		}
		try {
			URI uri = URI.create(value.trim());
			String scheme = uri.getScheme();
			String host = uri.getHost();
			if (scheme == null || host == null) {
				return sanitizeText(value.split("[?#]", 2)[0], 240);
			}
			StringBuilder safe = new StringBuilder(scheme).append("://").append(host);
			if (uri.getPort() > -1) {
				safe.append(':').append(uri.getPort());
			}
			String rawPath = uri.getRawPath();
			if (rawPath != null && !rawPath.isBlank() && !"/".equals(rawPath)) {
				String decodedPath = URLDecoder.decode(rawPath, StandardCharsets.UTF_8);
				safe.append(sanitizeText(decodedPath, 180));
			}
			return safe.toString();
		} catch (Exception e) {
			return sanitizeText(value.split("[?#]", 2)[0], 240);
		}
	}

	static String safeSlug(String value) {
		String slug = sanitizeText(value, 100).toLowerCase().replaceAll("[^a-z0-9]+", "-")
				.replaceAll("^-+|-+$", "");
		return slug.isBlank() ? "playwright-recording" : slug;
	}
}
