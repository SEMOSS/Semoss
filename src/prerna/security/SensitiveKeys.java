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

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import prerna.util.Constants;
import prerna.util.Utility;

/**
 * The one rule for whether a setting holds a secret, judged by its key name.
 * The smss editor, the RDF_Map editor, the log file masking, the audit trail
 * and the secret values error text is checked against all use it, so a key that
 * is secret in one of them is secret in all of them.
 * <p>
 * Names are compared without case and without separators, so
 * {@code SECRET_KEY}, {@code secretKey} and {@code secret-key} are one name. A
 * key is secret when it is a listed name, ends with a listed ending, or is
 * named in the RDF_Map {@link Constants#SENSITIVE_KEYS} setting.
 */
public final class SensitiveKeys {

	private static final Set<String> SENSITIVE_NAMES = Set.of("password", "passwd", "pwd", "pass", "secret", "token",
			"salt", "hash", "otp", "pin", "credential", "credentials", "authorization", "cookie", "cookies",
			"sessionid", "jsessionid", "apikey", "accesskey", "secretkey", "privatekey", "openaikey", "azkey",
			"azprimarykey", "headers", "connectionstring", "connectionstr", "awssecretaccesskey", "creds");

	private static final List<String> SENSITIVE_ENDINGS = List.of("password", "passwd", "pwd", "secret", "token",
			"apikey", "accesskey", "secretkey", "privatekey", "accountkey", "serviceaccountkey", "serviceaccountjson",
			"credentials", "creds", "connstring", "connectionstring", "sasurl", "sessionid");

	private static volatile AdditionalNames additionalNames = new AdditionalNames(null, Set.of());

	private SensitiveKeys() {

	}

	/**
	 * Whether the value stored under a key is a secret.
	 *
	 * @param key a setting, smss or JSON key
	 * @return true when the key's value must not be shown
	 */
	public static boolean isSensitiveKey(String key) {
		String name = normalize(key);
		if (name.isEmpty()) {
			return false;
		}
		if (SENSITIVE_NAMES.contains(name)) {
			return true;
		}
		for (String ending : SENSITIVE_ENDINGS) {
			if (name.endsWith(ending)) {
				return true;
			}
		}
		return additionalNames().contains(name);
	}

	/**
	 * The form a key name is compared in: lower case, letters and digits only.
	 *
	 * @param key a key name
	 * @return the normalized name, empty for a null key
	 */
	static String normalize(String key) {
		if (key == null) {
			return "";
		}
		StringBuilder name = new StringBuilder(key.length());
		for (char c : key.toLowerCase(Locale.ROOT).toCharArray()) {
			if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
				name.append(c);
			}
		}
		return name.toString();
	}

	/**
	 * The names an admin added in the RDF_Map. They are parsed again only when the
	 * setting changes.
	 *
	 * @return the normalized added names
	 */
	private static Set<String> additionalNames() {
		String setting = Utility.getDIHelperProperty(Constants.SENSITIVE_KEYS);
		AdditionalNames current = additionalNames;
		if (setting == null ? current.setting() == null : setting.equals(current.setting())) {
			return current.names();
		}
		Set<String> names = new HashSet<>();
		if (setting != null) {
			for (String name : setting.split(",")) {
				String normalized = normalize(name);
				if (!normalized.isEmpty()) {
					names.add(normalized);
				}
			}
		}
		AdditionalNames parsed = new AdditionalNames(setting, Set.copyOf(names));
		additionalNames = parsed;
		return parsed.names();
	}

	/**
	 * The RDF_Map setting as last read, with the names parsed from it.
	 *
	 * @param setting the raw setting, or null when it is not set
	 * @param names   the normalized names in it
	 */
	private record AdditionalNames(String setting, Set<String> names) {
	}
}
