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
package prerna.reactor.shortcuts.fileupload.job;

import java.time.LocalDate;

public class ComparatorUtil {
	public static boolean compare(Object actual, String operator, String expected) {

		if (actual == null) {
			return false;
		}

		String actualValue = actual.toString();

		switch (operator) {

		case "EQUALS":
			return actualValue.equalsIgnoreCase(expected);

		case "NOT_EQUALS":
			return !actualValue.equalsIgnoreCase(expected);

		case "CONTAINS":
			return actualValue.contains(expected);

		case "STARTS_WITH":
			return actualValue.startsWith(expected);

		case "ENDS_WITH":
			return actualValue.endsWith(expected);

		case "GT":
			return Double.parseDouble(actualValue) > Double.parseDouble(expected);

		case "LT":
			return Double.parseDouble(actualValue) < Double.parseDouble(expected);

		case "GTE":
			return Double.parseDouble(actualValue) >= Double.parseDouble(expected);

		case "LTE":
			return Double.parseDouble(actualValue) <= Double.parseDouble(expected);

		case "EXISTS":
			return actual != null;

		case "TODAY_MATCH":

			LocalDate date = LocalDate.parse(actualValue);

			LocalDate today = LocalDate.now();

			return date.getMonth() == today.getMonth() && date.getDayOfMonth() == today.getDayOfMonth();

		default:
			return false;
		}
	}
}
