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
package prerna.remoteviewer.security;

import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Validates target URLs before a browser session opens them.
 * Blocks private networks, metadata services, and dangerous schemes.
 */
public class UrlSafetyValidator {

	private static final Logger classLogger = LogManager.getLogger(UrlSafetyValidator.class);

	private static final List<String> ALLOWED_SCHEMES = Arrays.asList("http", "https");

	/** RFC-1918 / loopback / APIPA / IPv6 loopback prefixes */
	private static final List<Pattern> BLOCKED_HOST_PATTERNS = Arrays.asList(
			Pattern.compile("^localhost$", Pattern.CASE_INSENSITIVE),
			Pattern.compile("^127\\..*"),
			Pattern.compile("^0\\.0\\.0\\.0$"),
			Pattern.compile("^10\\..*"),
			Pattern.compile("^172\\.(1[6-9]|2[0-9]|3[01])\\..*"),
			Pattern.compile("^192\\.168\\..*"),
			Pattern.compile("^169\\.254\\..*"),   // APIPA / AWS metadata
			Pattern.compile("^::1$"),             // IPv6 loopback
			Pattern.compile("^fc00:.*", Pattern.CASE_INSENSITIVE),
			Pattern.compile("^fd.*", Pattern.CASE_INSENSITIVE)
	);

	private UrlSafetyValidator() {}

	/**
	 * Returns {@code true} when the URL is safe to open in a remote browser.
	 *
	 * @param rawUrl the URL supplied by the user
	 * @throws IllegalArgumentException with a user-facing message when unsafe
	 */
	public static void validate(String rawUrl) {
		if (rawUrl == null || rawUrl.isBlank()) {
			throw new IllegalArgumentException("URL must not be empty");
		}

		URI uri;
		try {
			uri = new URI(rawUrl.trim()).normalize();
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid URL: " + rawUrl);
		}

		String scheme = uri.getScheme();
		if (scheme == null || !ALLOWED_SCHEMES.contains(scheme.toLowerCase())) {
			throw new IllegalArgumentException("URL scheme must be http or https");
		}

		String host = uri.getHost();
		if (host == null || host.isBlank()) {
			throw new IllegalArgumentException("URL must contain a valid host");
		}

		// Check the literal hostname
		for (Pattern p : BLOCKED_HOST_PATTERNS) {
			if (p.matcher(host).matches()) {
				classLogger.warn("Blocked private/reserved host: {}", host);
				throw new IllegalArgumentException("URL targets a blocked host");
			}
		}

		// Resolve DNS and check the resolved IP
		boolean blockPrivateNetworks = !"false".equalsIgnoreCase(
				System.getenv("REMOTE_BROWSER_BLOCK_PRIVATE_NETWORKS"));
		if (blockPrivateNetworks) {
			try {
				InetAddress addr = InetAddress.getByName(host);
				if (addr.isLoopbackAddress() || addr.isSiteLocalAddress()
						|| addr.isLinkLocalAddress() || addr.isAnyLocalAddress()) {
					classLogger.warn("Blocked resolved private address for host: {}", host);
					throw new IllegalArgumentException("URL resolves to a blocked address");
				}
			} catch (IllegalArgumentException e) {
				throw e;
			} catch (Exception e) {
				// DNS resolution failure — block to be safe
				classLogger.warn("DNS resolution failed for host '{}', blocking. Error: {}", host, e.getMessage());
				throw new IllegalArgumentException("URL host could not be resolved");
			}
		}
	}
}
