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
import java.util.List;
import java.util.Map;

import prerna.io.connector.ConnectorOutput;

/**
 * Somebody a calendar is shared with, and what they may do with it.
 *
 * @param id                   the provider's id for the permission
 * @param role                 what they may do: freeBusyRead, limitedRead,
 *                             read, write, owner, or for Outlook a delegate
 *                             role
 * @param address              who it is shared with
 * @param name                 their display name
 * @param allowedRoles         the roles they could be given, when the provider
 *                             says
 * @param isInsideOrganization whether they are inside the user's organization
 * @param isRemovable          whether the share can be taken away
 * @param isDelegate           whether they may also answer invitations on the
 *                             owner's behalf
 */
public record CalendarPermission(String id, String role, String address, String name, List<String> allowedRoles,
		boolean isInsideOrganization, boolean isRemovable, boolean isDelegate) {

	/**
	 * @return the permission as a reactor answers with it
	 */
	public Map<String, Object> toMap() {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("id", this.id);
		ConnectorOutput.putIfPresent(output, "role", this.role);
		ConnectorOutput.putIfPresent(output, "address", this.address);
		ConnectorOutput.putIfPresent(output, "name", this.name);
		ConnectorOutput.putIfPresent(output, "allowedRoles", this.allowedRoles);
		output.put("isInsideOrganization", this.isInsideOrganization);
		output.put("isRemovable", this.isRemovable);
		output.put("isDelegate", this.isDelegate);
		return output;
	}
}
