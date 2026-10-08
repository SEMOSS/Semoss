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
package prerna.io.connector.calendar;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import prerna.io.connector.ConnectorTimes;

/**
 * When an event starts or ends: a moment, or for a whole day event, a date.
 *
 * @param instant the moment, or null for a whole day
 * @param date    the date of a whole day event, or null for a moment
 */
public record EventTime(Instant instant, LocalDate date) {

	/**
	 * @param instant the moment
	 * @return the time
	 */
	public static EventTime of(Instant instant) {
		return instant == null ? null : new EventTime(instant, null);
	}

	/**
	 * @param date the day
	 * @return the time of a whole day event
	 */
	public static EventTime ofDate(LocalDate date) {
		return date == null ? null : new EventTime(null, date);
	}

	/**
	 * @return whether this is a whole day rather than a moment
	 */
	public boolean isDate() {
		return this.date != null;
	}

	/**
	 * @param zone the zone to read the time in
	 * @return the time as a clock in that zone shows it, which is midnight for a
	 *         whole day
	 */
	public LocalDateTime localIn(ZoneId zone) {
		return isDate() ? this.date.atStartOfDay() : LocalDateTime.ofInstant(this.instant, zone);
	}

	/**
	 * @return the time as a reactor answers with it: UTC for a moment, or the date
	 *         for a whole day
	 */
	public String format() {
		return isDate() ? this.date.toString() : ConnectorTimes.format(this.instant);
	}
}
