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
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

/**
 * The one value the platform writes wherever a session id would otherwise be
 * seen: folder names, logs, the audit and user tracking tables, and the
 * inference logs. A session id is all a browser needs to act as its user, so
 * only the session's own lookups keep the real id, and everything people or
 * other processes read gets its hash.
 *
 * <p>
 * The hash is an HMAC-SHA256 keyed by the RDF_Map setting
 * {@link #SALT_PROPERTY}, written as 64 lowercase hex characters. Every server
 * of a cluster has the same setting, so a session gets the same hash on each of
 * them. Without the setting it is a plain SHA-256 of the id.
 */
public final class SessionIds {

	private static final Logger classLogger = LogManager.getLogger(SessionIds.class);

	/** The RDF_Map setting that keys the hash */
	public static final String SALT_PROPERTY = "LOG_HASH_SALT";

	/**
	 * The values the platform puts where a session id goes when there is no
	 * session. They name no session, so they are written as they are.
	 */
	public static final Set<String> PLACEHOLDERS = Set.of("UNKNOWN", "STARTUP", "SHUTDOWN", "NO_SESSION_ID", "NULL");

	private static final String HMAC = "HmacSHA256";
	private static final AtomicBoolean WARNED_WITHOUT_SALT = new AtomicBoolean();

	private SessionIds() {

	}

	/**
	 * The hash written in place of a session id. The same session always gets the
	 * same hash, and the hash does not give the session id back.
	 *
	 * @param sessionId the session id, or a placeholder such as {@code UNKNOWN}
	 * @return the hash, the placeholder as it is, or null for no session id
	 */
	public static String hash(String sessionId) {
		if (sessionId == null || sessionId.isBlank()) {
			return null;
		}
		if (PLACEHOLDERS.contains(sessionId)) {
			return sessionId;
		}
		byte[] value = sessionId.getBytes(StandardCharsets.UTF_8);
		String salt = Utility.getDIHelperProperty(SALT_PROPERTY);
		try {
			if (salt != null && !salt.isEmpty()) {
				Mac mac = Mac.getInstance(HMAC);
				mac.init(new SecretKeySpec(salt.getBytes(StandardCharsets.UTF_8), HMAC));
				return HexFormat.of().formatHex(mac.doFinal(value));
			}
			if (WARNED_WITHOUT_SALT.compareAndSet(false, true)) {
				classLogger.warn("{} is not set in RDF_Map, so session ids are hashed with plain SHA-256",
						SALT_PROPERTY);
			}
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			throw new IllegalStateException("Unable to hash a session id", e);
		}
	}
}
