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
package prerna.logging;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;

import prerna.util.Utility;

/**
 * Pseudonymizes an MDC field via HMAC-SHA256, producing a stable 16-char hex
 * token that can be correlated across log events without exposing the real value.
 *
 * Usage in semoss-log-template.json:
 *   "userId": {"$resolver": "pattern", "pattern": "%hashMdc{userId}"}
 *
 * Requires LOG_HASH_SALT in RDF_Map.properties. Without it, falls back to
 * plain SHA-256 (still pseudonymous but vulnerable to rainbow tables).
 */
@Plugin(name = "HashMdc", category = PatternConverter.CATEGORY)
@ConverterKeys({ "hashMdc" })
public class HashMdcPatternConverter extends LogEventPatternConverter {

	private static final Logger classLogger = LogManager.getLogger(HashMdcPatternConverter.class);
	private static final String LOG_HASH_SALT_PROPERTY = "LOG_HASH_SALT";
	// 16 hex chars = 8 bytes = 64-bit token, sufficient for correlation
	private static final int OUTPUT_HEX_LENGTH = 16;

	private final String mdcKey;

	protected HashMdcPatternConverter(String mdcKey) {
		super("hashMdc", "hashMdc");
		this.mdcKey = mdcKey;
	}

	@PluginFactory
	public static HashMdcPatternConverter newInstance(String[] options) {
		String key = (options != null && options.length > 0 && options[0] != null) ? options[0].trim() : "";
		return new HashMdcPatternConverter(key);
	}

	@Override
	public void format(LogEvent event, StringBuilder toAppendTo) {
		if (mdcKey.isEmpty()) {
			return;
		}
		String value = event.getContextData().getValue(mdcKey);
		if (value == null || value.isEmpty()) {
			return;
		}
		toAppendTo.append(pseudonymize(value));
	}

	private static String pseudonymize(String value) {
		try {
			String salt = Utility.getDIHelperProperty(LOG_HASH_SALT_PROPERTY);
			byte[] raw;
			if (salt != null && !salt.isEmpty()) {
				Mac mac = Mac.getInstance("HmacSHA256");
				mac.init(new SecretKeySpec(salt.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
				raw = mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
			} else {
				// salt not configured. plain SHA-256, still one-way but rainbow-table susceptible
				classLogger.warn("LOG_HASH_SALT not set in RDF_Map.properties; hashMdc falling back to unsalted SHA-256");
				raw = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
			}
			return toHex(raw, OUTPUT_HEX_LENGTH);
		} catch (NoSuchAlgorithmException | InvalidKeyException e) {
			classLogger.error("hashMdc pseudonymization failed for key '{}': {}", value, e.getMessage());
			return "";
		}
	}

	private static String toHex(byte[] bytes, int maxHexChars) {
		StringBuilder sb = new StringBuilder(maxHexChars);
		for (byte b : bytes) {
			sb.append(String.format("%02x", b));
			if (sb.length() >= maxHexChars) {
				break;
			}
		}
		return sb.toString();
	}

}
