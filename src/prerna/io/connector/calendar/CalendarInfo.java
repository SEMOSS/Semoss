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

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.io.connector.ConnectorOutput;

/**
 * One calendar the signed in user can see: their own, or one somebody shared
 * with them.
 *
 * @param id                  what the calendar reactors take as calendarId
 * @param name                its name
 * @param color               the color it is shown in
 * @param owner               the owner's address
 * @param ownerName           the owner's display name
 * @param canEdit             whether the user can write events to it
 * @param canShare            whether the user can share it
 * @param canViewPrivateItems whether the user can see its private events
 * @param isDefaultCalendar   whether it is the user's own default calendar
 * @param isSharedWithMe      whether somebody else owns it, when that can be
 *                            told
 */
public record CalendarInfo(String id, String name, String color, String owner, String ownerName, boolean canEdit,
		boolean canShare, boolean canViewPrivateItems, boolean isDefaultCalendar, Boolean isSharedWithMe) {

	/**
	 * @return the calendar as a reactor answers with it
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("id", this.id);
		ConnectorOutput.putIfPresent(output, "name", this.name);
		ConnectorOutput.putIfPresent(output, "color", this.color);
		ConnectorOutput.putIfPresent(output, "owner", this.owner);
		ConnectorOutput.putIfPresent(output, "ownerName", this.ownerName);
		output.put("canEdit", this.canEdit);
		output.put("canShare", this.canShare);
		output.put("canViewPrivateItems", this.canViewPrivateItems);
		output.put("isDefaultCalendar", this.isDefaultCalendar);
		ConnectorOutput.putIfPresent(output, "isSharedWithMe", this.isSharedWithMe);
		return output;
	}
}
