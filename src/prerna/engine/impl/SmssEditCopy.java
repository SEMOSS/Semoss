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
package prerna.engine.impl;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.text.StringEscapeUtils;

import prerna.engine.api.IEngine;
import prerna.io.connector.secrets.ISecrets;
import prerna.io.connector.secrets.SecretsFactory;
import prerna.util.ContentChangedException;

/**
 * The copy of an engine's or project's smss that its owners, and admins, read
 * to edit: the smss file and the values the secret store keeps for it, with
 * every secret masked. It comes with {@link #getSmssHash()}, a hash of the smss
 * as it is now, which a save hands back so a save made from an older copy is
 * refused ({@link #requireCurrent(IEngine, String)}).
 *
 * <p>
 * The hash is the SHA-256 of the file's bytes and of the secret store's values
 * in key order. It is worked out on the server from what is stored, never from
 * the text the editor sends back, so the masking does not change it. With a
 * secret store, a save changes the store and not the file, so the hash covers
 * both.
 */
public final class SmssEditCopy {

	/** The key of the masked smss text */
	public static final String SMSS = "smss";

	/** The key of the hash a save hands back */
	public static final String SMSS_HASH = "smssHash";

	/** The error code of a save made from an older copy */
	public static final String SMSS_CHANGED = "SMSS_CHANGED";

	private static final String SECRET_SECTION_HEADER = "#Comments below this section will be lost\n"
			+ "#These values are stored in a secret store\n";

	private final String smss;
	private final String smssHash;

	private SmssEditCopy(String smss, String smssHash) {
		this.smss = smss;
		this.smssHash = smssHash;
	}

	/**
	 * Reads the copy of an engine's or project's smss as it is now.
	 *
	 * @param engine the engine or project, as its owner or an admin reads it
	 * @return the masked smss and its hash
	 * @throws IllegalArgumentException if the smss file cannot be found or read
	 */
	public static SmssEditCopy read(IEngine engine) {
		String smssFilePath = engine.getSmssFilePath();
		Path smssFile = smssFilePath == null ? null : Paths.get(smssFilePath);
		if (smssFile == null || !Files.isRegularFile(smssFile)) {
			throw new IllegalArgumentException("Could not find smss file for " + describe(engine)
					+ ". Please reach out to an administrator for assistance");
		}
		byte[] fileBytes;
		try {
			fileBytes = Files.readAllBytes(smssFile);
		} catch (IOException e) {
			throw new IllegalArgumentException("An error occurred reading the current smss details of "
					+ describe(engine) + ". Detailed message = " + e.getMessage(), e);
		}
		Map<String, String> secrets = secretsOf(engine);

		StringBuilder text = new StringBuilder(new String(fileBytes, StandardCharsets.UTF_8));
		if (!secrets.isEmpty()) {
			if (text.length() > 0 && text.charAt(text.length() - 1) != '\n') {
				text.append('\n');
			}
			text.append(SECRET_SECTION_HEADER);
			for (Map.Entry<String, String> secret : secrets.entrySet()) {
				text.append('\n').append(secret.getKey()).append('\t')
						.append(StringEscapeUtils.escapeJava(secret.getValue()));
			}
		}
		return new SmssEditCopy(SmssUtilities.concealSmssSensitiveInfo(text.toString()), hash(fileBytes, secrets));
	}

	/**
	 * Refuses a save unless it was made from the copy of the smss as it is now.
	 * Call it while holding the engine's or project's lock, so nothing changes the
	 * smss between the check and the save.
	 *
	 * @param engine   the engine or project being saved
	 * @param smssHash the hash the editor read with the smss
	 * @throws IllegalArgumentException if no hash is given
	 * @throws ContentChangedException  if the smss changed since that copy was read
	 */
	public static void requireCurrent(IEngine engine, String smssHash) {
		if (smssHash == null || smssHash.isBlank()) {
			throw new IllegalArgumentException("Saving the smss of " + describe(engine) + " needs the " + SMSS_HASH
					+ " that came with it. Load the smss again and make the change there.");
		}
		if (!read(engine).getSmssHash().equals(smssHash.trim())) {
			throw new ContentChangedException(SMSS_CHANGED, "The smss of " + describe(engine)
					+ " changed since you opened it. Load the latest version to make your change; your unsaved changes will be lost.");
		}
	}

	/**
	 * The masked smss text, the same text the editor shows.
	 *
	 * @return the smss with every secret masked
	 */
	public String getSmss() {
		return this.smss;
	}

	/**
	 * The hash of the smss as it was read, which a save hands back.
	 *
	 * @return the SHA-256 of the smss file and its secret store values, in hex
	 */
	public String getSmssHash() {
		return this.smssHash;
	}

	/**
	 * The copy as a reactor returns it.
	 *
	 * @return {@link #SMSS} and {@link #SMSS_HASH}
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> copy = new LinkedHashMap<>();
		copy.put(SMSS, this.smss);
		copy.put(SMSS_HASH, this.smssHash);
		return copy;
	}

	private static Map<String, String> secretsOf(IEngine engine) {
		Map<String, String> secrets = new TreeMap<>();
		ISecrets secretStore = SecretsFactory.getSecretConnector();
		if (secretStore == null) {
			return secrets;
		}
		Map<String, Object> engineSecrets = secretStore.getEngineSecrets(engine.getCatalogType(), engine.getEngineId(),
				engine.getEngineName());
		if (engineSecrets != null) {
			for (Map.Entry<String, Object> secret : engineSecrets.entrySet()) {
				secrets.put(secret.getKey(), String.valueOf(secret.getValue()));
			}
		}
		return secrets;
	}

	private static String hash(byte[] fileBytes, Map<String, String> secrets) {
		MessageDigest digest;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is not available to hash the smss", e);
		}
		digest.update(fileBytes);
		for (Map.Entry<String, String> secret : secrets.entrySet()) {
			digest.update((byte) 0);
			digest.update(secret.getKey().getBytes(StandardCharsets.UTF_8));
			digest.update((byte) 0);
			digest.update(secret.getValue().getBytes(StandardCharsets.UTF_8));
		}
		return HexFormat.of().formatHex(digest.digest());
	}

	private static String describe(IEngine engine) {
		String kind = engine.getCatalogType() == IEngine.CATALOG_TYPE.PROJECT ? "project " : "engine ";
		return kind + engine.getEngineId();
	}
}
