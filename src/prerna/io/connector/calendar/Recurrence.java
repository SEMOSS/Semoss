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

import java.time.LocalDate;
import java.util.List;
import java.util.Locale;

/**
 * How an event repeats.
 *
 * <p>
 * A series repeats on the weekday, the day of the month, or the date its first
 * sitting falls on, once each day, week, month or year, until the date given,
 * or with no end when none is given.
 * </p>
 *
 * @param frequency one of {@link #DAILY}, {@link #WEEKLY}, {@link #MONTHLY} or
 *                  {@link #YEARLY}
 * @param until     the last day it can repeat on, or null for no end
 */
public record Recurrence(String frequency, LocalDate until) {

	public static final String DAILY = "daily";
	public static final String WEEKLY = "weekly";
	public static final String MONTHLY = "monthly";
	public static final String YEARLY = "yearly";

	/** Every frequency a series can repeat at. */
	public static final List<String> FREQUENCIES = List.of(DAILY, WEEKLY, MONTHLY, YEARLY);

	/**
	 * @param frequency how often it repeats, however the caller capitalized it
	 * @param until     the last day it can repeat on, or null
	 * @return the recurrence
	 * @throws IllegalArgumentException when the frequency is not one of
	 *                                  {@link #FREQUENCIES}
	 */
	public static Recurrence of(String frequency, LocalDate until) {
		String normalized = frequency.trim().toLowerCase(Locale.ROOT);
		if (!FREQUENCIES.contains(normalized)) {
			throw new IllegalArgumentException(
					"recurrence must be one of " + FREQUENCIES + " but received: " + frequency);
		}
		return new Recurrence(normalized, until);
	}
}
