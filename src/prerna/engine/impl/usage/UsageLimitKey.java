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
package prerna.engine.impl.usage;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import prerna.engine.api.IEngine.CATALOG_TYPE;

/**
 * Catalog is mandatory even for totals, so database and model budgets never
 * mix.
 */
public record UsageLimitKey(CATALOG_TYPE catalog, Source source, String engineId, String subjectId,
		String subjectType) {
	// Counter identities must not depend on application adapters or
	// pretty-printing.
	private static final Gson KEY_GSON = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();

	public enum Source {
		DIRECT, TEAM, PUBLIC, ENGINE_TOTAL, USER_TOTAL, TEAM_TOTAL, PLATFORM
	}

	public boolean ceiling() {
		return source == Source.ENGINE_TOTAL || source == Source.TEAM_TOTAL;
	}

	public void validate() {
		if (catalog == null || source == null)
			throw new IllegalArgumentException("Catalog and source are required");
		boolean onEngine = source == Source.DIRECT || source == Source.TEAM || source == Source.PUBLIC
				|| source == Source.ENGINE_TOTAL;
		boolean subject = source == Source.DIRECT || source == Source.TEAM || source == Source.USER_TOTAL
				|| source == Source.TEAM_TOTAL;
		if (onEngine != (engineId != null && !engineId.isBlank()) || (!onEngine && engineId != null))
			throw new IllegalArgumentException("Invalid engine scope");
		if (subject != (subjectId != null && !subjectId.isBlank())
				|| (subject && (subjectType == null || subjectType.isBlank()))
				|| (!subject && (subjectId != null || subjectType != null)))
			throw new IllegalArgumentException("Invalid usage subject");
	}

	public String id() {
		return digest(Arrays.asList(catalog, source, engineId, subjectId, subjectType));
	}

	public static String digest(Object value) {
		try {
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
					.digest(KEY_GSON.toJson(value).getBytes(StandardCharsets.UTF_8)));
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}
}
