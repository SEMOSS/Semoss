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
package prerna.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Helpers for OAuth2 PKCE (Proof Key for Code Exchange, RFC 7636). The caller
 * generates a high-entropy {@code code_verifier} before redirecting to the
 * provider, sends the derived {@code code_challenge} (S256) on the authorize
 * request, and then sends the original verifier back on the token exchange.
 */
public class PKCEUtil {

	/** S256 is the only challenge method used here. */
	public static final String CODE_CHALLENGE_METHOD = "S256";

	private static final SecureRandom RANDOM = new SecureRandom();
	private static final Base64.Encoder URL_ENCODER = Base64.getUrlEncoder().withoutPadding();

	private PKCEUtil() {
		// static utility
	}

	/**
	 * Generate a fresh PKCE {@code code_verifier}: 32 random bytes, base64url
	 * encoded without padding (a 43-character high-entropy string).
	 *
	 * @return a new code verifier
	 */
	public static String generateCodeVerifier() {
		byte[] bytes = new byte[32];
		RANDOM.nextBytes(bytes);
		return URL_ENCODER.encodeToString(bytes);
	}

	/**
	 * Derive the S256 {@code code_challenge} for a verifier:
	 * {@code base64url(sha256(verifier))} without padding.
	 *
	 * @param codeVerifier the verifier returned by {@link #generateCodeVerifier()}
	 * @return the S256 code challenge
	 */
	public static String codeChallenge(String codeVerifier) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hash = digest.digest(codeVerifier.getBytes(StandardCharsets.US_ASCII));
			return URL_ENCODER.encodeToString(hash);
		} catch (NoSuchAlgorithmException e) {
			// SHA-256 is guaranteed to be available on every JVM
			throw new IllegalStateException("SHA-256 not available for PKCE code challenge", e);
		}
	}

}
