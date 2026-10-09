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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.io.connector.ConnectorOutput;
import prerna.io.connector.ConnectorTimes;

/**
 * When one person is busy over a window.
 *
 * @param address the person, by email address
 * @param busy    the times they are not free, earliest first
 * @param error   what went wrong reading their calendar, or null
 */
public record Availability(String address, List<Availability.Busy> busy, String error) {

	public Availability {
		busy = busy == null ? List.of() : List.copyOf(busy);
	}

	/**
	 * One stretch of time somebody is not free.
	 *
	 * @param start    when it starts
	 * @param end      when it ends
	 * @param status   busy, tentative, oof or workingElsewhere
	 * @param subject  what they are busy with, when their calendar shares that
	 * @param location where, when their calendar shares that
	 */
	public record Busy(Instant start, Instant end, String status, String subject, String location) {

		/**
		 * @return the stretch as a reactor answers with it
		 */
		public Map<String, Object> toMap() {
			Map<String, Object> output = new LinkedHashMap<>();
			output.put("start", ConnectorTimes.format(this.start));
			output.put("end", ConnectorTimes.format(this.end));
			output.put("status", this.status);
			ConnectorOutput.putIfPresent(output, "subject", this.subject);
			ConnectorOutput.putIfPresent(output, "location", this.location);
			return output;
		}
	}

	/**
	 * @return the person's availability as a reactor answers with it
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("address", this.address);
		output.put("busy", this.busy.stream().map(Busy::toMap).toList());
		ConnectorOutput.putIfPresent(output, "error", this.error);
		return output;
	}
}
