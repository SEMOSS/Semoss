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
package prerna.date.reactor;

import java.util.Calendar;

import prerna.date.SemossDate;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateReactor extends AbstractReactor {

	private static final String DEFAULT_FORMAT = "yyyy-MM-dd";

	public DateReactor() {
		this.keysToGet = new String[] { "date", "format" };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		SemossDate date = null;
		String pattern = DEFAULT_FORMAT;

		/*
		 * If there is no date input, then we will grab todays date If there is a date
		 * input, we assume it is yyyy-MM-dd format If there is a date input and a
		 * format, we will use that format
		 */

		// determine if we should use the default format
		// or the user defined format
		if (this.keyValue.containsKey(this.keysToGet[1])) {
			pattern = this.keyValue.get(this.keysToGet[1]);
		}

		if (this.keyValue.containsKey(this.keysToGet[0])) {
			String strDate = this.keyValue.get(this.keysToGet[0]);
			if (!strDate.isBlank()) {
				date = new SemossDate(strDate, pattern);
				date.getZonedDateTime();
			}
		}
		if (date == null) {
			// the user hasn't specified a date
			date = new SemossDate(Calendar.getInstance().getTime(), pattern);
		}

		return new NounMetadata(date, PixelDataType.CONST_DATE);
	}

	@Override
	public String getReactorDescription() {
		return "Get todays date or return a date based on a specific date input and format";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if ("date".equals(key)) {
			return "A specific date to return. This is a string and assumes a date of yyyy-MM-dd";
		} else if ("format".equals(key)) {
			return "A specified format for the date parameter to parse. This should be a Java compliant format";
		}
		return super.getDescriptionForKey(key);
	}

}
