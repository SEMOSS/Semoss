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
package prerna.engine.api;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Content modalities supported by model metadata and message validation.
 * Metadata is persisted using the enum names in upper case; catalog files use
 * the lower-case form returned by {@link #getCatalogName()}.
 */
public enum ModelModalityEnum {
	TEXT, IMAGE, AUDIO, VIDEO, VECTOR, FILE, PDF;

	private static final Set<String> NAMES;

	static {
		Set<String> names = new LinkedHashSet<>();
		Arrays.stream(values()).map(ModelModalityEnum::name).forEach(names::add);
		NAMES = Collections.unmodifiableSet(names);
	}

	/**
	 * Parse a metadata or catalog modality name case-insensitively.
	 *
	 * @param value modality name
	 * @return the matching modality
	 * @throws IllegalArgumentException when the value is null, blank, or unknown
	 */
	public static ModelModalityEnum fromName(String value) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException("Unsupported modality " + value);
		}
		String normalizedValue = value.trim().toUpperCase(Locale.ROOT);
		try {
			return valueOf(normalizedValue);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Unsupported modality " + normalizedValue, e);
		}
	}

	/**
	 * @return the upper-case names accepted in model metadata
	 */
	public static Set<String> names() {
		return NAMES;
	}

	/**
	 * @return the lower-case spelling used by the static model catalog
	 */
	public String getCatalogName() {
		return name().toLowerCase(Locale.ROOT);
	}
}
