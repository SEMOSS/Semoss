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
package prerna.io.connector;

import java.util.Map;

/**
 * How the mail and calendar records write themselves out, so every record
 * leaves out a field the same way and cuts a long body short the same way.
 */
public final class ConnectorOutput {

	/** What a body cut short ends with, so a reader can see it was cut. */
	public static final String TRUNCATED_SUFFIX = " ... [truncated]";

	private ConnectorOutput() {

	}

	/**
	 * Set a key only when there is something to set it to, so a caller reading the
	 * output does not have to tell a null apart from an absent field.
	 *
	 * @param output the map being built
	 * @param key    the key to set
	 * @param value  the value, ignored when null
	 */
	public static void putIfPresent(Map<String, Object> output, String key, Object value) {
		if (value != null) {
			output.put(key, value);
		}
	}

	/**
	 * Set a body, cut short when it runs longer than asked for, with
	 * {@code <key>Truncated} set when it was.
	 *
	 * @param output   the map being built
	 * @param key      the key to set
	 * @param text     the text
	 * @param maxChars the longest text to write before cutting it, or 0 to write
	 *                 whatever length it is
	 */
	public static void putText(Map<String, Object> output, String key, String text, int maxChars) {
		String value = text == null ? "" : text;
		if (maxChars > 0 && value.length() > maxChars) {
			value = value.substring(0, maxChars) + TRUNCATED_SUFFIX;
			output.put(key + "Truncated", true);
		}
		output.put(key, value);
	}
}
