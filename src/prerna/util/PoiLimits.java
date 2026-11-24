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
package prerna.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.util.IOUtils;

/**
 * The instance's limits for the Office files Apache POI opens. POI keeps these
 * limits in static fields that hold for every document read on the server, so
 * start up sets them once, after RDF_Map is loaded, and this is the one place
 * the platform sets them. Each limit is read from RDF_Map, or from the
 * environment variable of the same name when RDF_Map does not set it.
 */
public final class PoiLimits {

	private static final Logger classLogger = LogManager.getLogger(PoiLimits.class);

	private static final long MB = 1024L * 1024L;

	/**
	 * The setting for the most entries one Office file (xlsx, docx, pptx) can hold.
	 * POI refuses a file with more entries as a possible zip bomb.
	 */
	public static final String MAX_FILE_COUNT_SETTING = "POI_MAX_FILE_COUNT";

	/**
	 * The most entries one Office file can hold when
	 * {@link #MAX_FILE_COUNT_SETTING} is not set.
	 */
	public static final long DEFAULT_MAX_FILE_COUNT = 5_000;

	/**
	 * The setting for the most MB POI reads into memory at once for one record or
	 * part of a document, in place of POI's own smaller limit for each kind of
	 * record.
	 */
	public static final String BYTE_ARRAY_MAX_MB_SETTING = "POI_BYTE_ARRAY_MAX_MB";

	/**
	 * The most MB POI reads into memory at once when
	 * {@link #BYTE_ARRAY_MAX_MB_SETTING} is not set.
	 */
	public static final long DEFAULT_BYTE_ARRAY_MAX_MB = 1_024;

	/**
	 * The largest value {@link #BYTE_ARRAY_MAX_MB_SETTING} takes, 2047 MB, since
	 * one Java byte array holds just under 2 GB.
	 */
	public static final long LARGEST_BYTE_ARRAY_MAX_MB = Integer.MAX_VALUE / MB;

	private PoiLimits() {
	}

	/**
	 * Sets POI's limits from the instance settings.
	 */
	public static void apply() {
		long maxFileCount = maxFileCount();
		long byteArrayMaxMb = byteArrayMaxMb();
		ZipSecureFile.setMaxFileCount(maxFileCount);
		IOUtils.setByteArrayMaxOverride((int) (byteArrayMaxMb * MB));
		classLogger.info("Office files can hold up to {} entries ({}) and {} MB in one record ({})", maxFileCount,
				MAX_FILE_COUNT_SETTING, byteArrayMaxMb, BYTE_ARRAY_MAX_MB_SETTING);
	}

	/**
	 * The most entries one Office file can hold: {@link #MAX_FILE_COUNT_SETTING},
	 * or {@link #DEFAULT_MAX_FILE_COUNT} when it is not set.
	 *
	 * @return the entry count
	 */
	static long maxFileCount() {
		return readPositive(MAX_FILE_COUNT_SETTING, DEFAULT_MAX_FILE_COUNT);
	}

	/**
	 * The most MB POI reads into memory at once:
	 * {@link #BYTE_ARRAY_MAX_MB_SETTING}, or {@link #DEFAULT_BYTE_ARRAY_MAX_MB}
	 * when it is not set, and at most {@link #LARGEST_BYTE_ARRAY_MAX_MB}.
	 *
	 * @return the size in MB
	 */
	static long byteArrayMaxMb() {
		long mb = readPositive(BYTE_ARRAY_MAX_MB_SETTING, DEFAULT_BYTE_ARRAY_MAX_MB);
		if (mb > LARGEST_BYTE_ARRAY_MAX_MB) {
			classLogger.warn("{} is above the {} MB one byte array holds, using {}", BYTE_ARRAY_MAX_MB_SETTING,
					LARGEST_BYTE_ARRAY_MAX_MB, LARGEST_BYTE_ARRAY_MAX_MB);
			return LARGEST_BYTE_ARRAY_MAX_MB;
		}
		return mb;
	}

	/**
	 * Reads a setting as a positive whole number, from RDF_Map or else the
	 * environment variable of the same name.
	 *
	 * @param setting      the setting name
	 * @param defaultValue the value when the setting is not set, or is not a
	 *                     positive whole number
	 * @return the value
	 */
	private static long readPositive(String setting, long defaultValue) {
		String value = Utility.getDIHelperProperty(setting);
		if (value == null || value.isBlank()) {
			value = System.getenv(setting);
		}
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		long number;
		try {
			number = Long.parseLong(value.trim());
		} catch (NumberFormatException e) {
			number = 0;
		}
		if (number < 1) {
			classLogger.warn("{} is not a positive whole number, using {}", setting, defaultValue);
			return defaultValue;
		}
		return number;
	}
}
