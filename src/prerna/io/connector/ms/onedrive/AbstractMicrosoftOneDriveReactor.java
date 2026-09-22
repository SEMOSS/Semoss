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
package prerna.io.connector.ms.onedrive;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.execptions.SemossPixelException;

/**
 * What every OneDrive reactor has in common.
 *
 * <p>
 * The keys naming a drive, an item and a path mean the same thing wherever they
 * appear, so they are named and described once here rather than once per
 * reactor. The readers of those keys are here for the same reason: every one of
 * these reactors has to turn pixel strings into numbers and lists, and the way
 * a bad value is reported should not depend on which reactor happened to read
 * it.
 * </p>
 *
 * <p>
 * Those three keys are also what makes a file somebody else shared reachable. A
 * reactor that only knew about {@code /me/drive} could describe the signed in
 * user's own files and nothing else, so every reactor here takes the
 * {@code driveId} that {@code MicrosoftOneDriveListSharedFiles} and the other
 * listings hand back.
 * </p>
 */
public abstract class AbstractMicrosoftOneDriveReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AbstractMicrosoftOneDriveReactor.class);

	protected static final String DRIVE_ID = "driveId";
	protected static final String ITEM_ID = "itemId";
	protected static final String PATH = "path";
	protected static final String SHARE_URL = "shareUrl";
	protected static final String CONFLICT_BEHAVIOR = "conflictBehavior";

	/**
	 * Read a key that has to be a positive whole number.
	 *
	 * @param key      the key to read
	 * @param fallback what it is when the caller left it out
	 * @param cap      the largest value allowed, which the value is held down to
	 * @return the value
	 */
	protected int positiveInt(String key, int fallback, int cap) {
		String value = trimToNull(this.keyValue.get(key));
		if (value == null) {
			return fallback;
		}
		int parsed;
		try {
			parsed = Integer.parseInt(value);
		} catch (NumberFormatException e) {
			classLogger.error("Invalid {} of '{}' passed to a Microsoft OneDrive reactor", key, value, e);
			throw new SemossPixelException(key + " must be a whole number.");
		}
		if (parsed <= 0) {
			throw new SemossPixelException(key + " must be greater than 0.");
		}
		if (parsed > cap) {
			classLogger.warn("A {} of {} was asked for, using {} instead", key, parsed, cap);
			return cap;
		}
		return parsed;
	}

	/**
	 * Read one set of values, which a caller may pass as several nouns, as one
	 * comma separated noun, or as any mixture of the two.
	 *
	 * @param key the key to read
	 * @return the values, or null when none were passed
	 */
	protected String[] values(String key) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		List<String> values = new ArrayList<>();
		for (int i = 0; i < grs.size(); i++) {
			Object value = grs.getNoun(i).getValue();
			if (value == null) {
				continue;
			}
			// a single value can still be a comma separated list, since that is how
			// somebody writing the pixel by hand tends to pass more than one
			for (String entry : value.toString().split(",")) {
				if (!entry.trim().isEmpty()) {
					values.add(entry.trim());
				}
			}
		}
		if (values.isEmpty()) {
			return null;
		}
		return values.toArray(new String[0]);
	}

	/**
	 * @param value the value to trim
	 * @return the value without surrounding space, or null when there is nothing
	 *         left of it
	 */
	protected static String trimToNull(String value) {
		if (value == null || value.trim().isEmpty()) {
			return null;
		}
		return value.trim();
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(DRIVE_ID)) {
			return "Optional id of the drive to work against, as returned by the OneDrive listings. The signed in user's own OneDrive is used when omitted, and another person's drive is named here to reach a file they shared.";
		} else if (key.equals(ITEM_ID)) {
			return "Optional id of the drive item, as returned by the OneDrive listings.";
		} else if (key.equals(PATH)) {
			return "Optional path relative to the drive root, or relative to the folder the item id names when both are given.";
		} else if (key.equals(SHARE_URL)) {
			return "Optional sharing link to the file, which stands in for the drive id, item id and path. Use this for a link somebody sent rather than a file already listed.";
		} else if (key.equals(CONFLICT_BEHAVIOR)) {
			return "Optional behavior when something of the same name is already there: fail, rename or replace.";
		}
		return super.getDescriptionForKey(key);
	}

}
