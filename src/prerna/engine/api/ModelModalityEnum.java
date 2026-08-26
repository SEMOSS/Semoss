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

import java.util.Locale;

import org.apache.tika.mime.MediaType;

/**
 * Content modalities supported by model metadata and message validation.
 * Metadata is persisted using the enum names in upper case; catalog files use
 * the lower-case form returned by {@link #getCatalogName()}.
 */
public enum ModelModalityEnum {
	TEXT, IMAGE, AUDIO, VIDEO, VECTOR, FILE, PDF;

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
	 * Classify a MIME type into the modality it represents. Parameters, casing
	 * and surrounding whitespace are tolerated (Tika base-type parsing). Returns
	 * null when the type is missing, unparseable, or too generic to classify
	 * (application/octet-stream) - callers treat unknown as unrestricted so a
	 * failed detection never rejects a request.
	 *
	 * @param mimeType a MIME type such as image/png or application/pdf
	 * @return the matching modality, or null when it cannot be determined
	 */
	public static ModelModalityEnum fromMimeType(String mimeType) {
		if (mimeType == null || mimeType.isBlank()) {
			return null;
		}
		MediaType mediaType = MediaType.parse(mimeType.trim().toLowerCase(Locale.ROOT));
		if (mediaType == null) {
			return null;
		}
		String family = mediaType.getType();
		String subtype = mediaType.getSubtype();
		if ("image".equals(family)) {
			return IMAGE;
		}
		if ("audio".equals(family)) {
			return AUDIO;
		}
		if ("video".equals(family)) {
			return VIDEO;
		}
		if ("pdf".equals(subtype) || "x-pdf".equals(subtype)) {
			return PDF;
		}
		if ("text".equals(family)) {
			return TEXT;
		}
		if ("octet-stream".equals(subtype)) {
			return null;
		}
		return FILE;
	}

	/**
	 * @return the lower-case spelling used by the static model catalog
	 */
	public String getCatalogName() {
		return name().toLowerCase(Locale.ROOT);
	}
}
