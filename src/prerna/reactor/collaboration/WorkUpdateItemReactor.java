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
package prerna.reactor.collaboration;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.collaboration.WorkItemUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkUpdateItem(itemId=["..."], status=["done"], snoozeUntil=["..."], priority=["P1"], title=["..."], dueAt=["..."], suggested=[false], reason=["..."]);
public class WorkUpdateItemReactor extends AbstractCollaborationReactor {

	private static final String ITEM_ID = "itemId";
	private static final String STATUS = "status";
	private static final String SNOOZE_UNTIL = "snoozeUntil";
	private static final String PRIORITY = "priority";
	private static final String TITLE = "title";
	private static final String DUE_AT = "dueAt";
	private static final String SUGGESTED = "suggested";
	private static final String REASON = "reason";

	public WorkUpdateItemReactor() {
		this.keysToGet = new String[] { ITEM_ID, STATUS, SNOOZE_UNTIL, PRIORITY, TITLE, DUE_AT, SUGGESTED, REASON };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String itemId = getString(ITEM_ID);
		if (itemId == null) {
			throw new IllegalArgumentException("Must pass an itemId");
		}
		Map<String, Object> changes = new LinkedHashMap<>();
		for (String key : new String[] { STATUS, SNOOZE_UNTIL, PRIORITY, TITLE, DUE_AT }) {
			if (getGenRowStruct(key) != null) {
				changes.put(key, getString(key));
			}
		}
		if (getGenRowStruct(SUGGESTED) != null) {
			changes.put(SUGGESTED, getBoolean(SUGGESTED));
		}
		return mapResult(WorkItemUtils.updateItem(user, itemId, changes, getString(REASON)));
	}

	@Override
	public String getReactorDescription() {
		return "Updates a Work item and records the change in its history; the result carries the changeId to undo";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ITEM_ID.equals(key)) {
			return "Work item id";
		} else if (STATUS.equals(key)) {
			return "open, waiting, done, dismissed, or snoozed";
		} else if (SNOOZE_UNTIL.equals(key)) {
			return "ISO-8601 time; alone it snoozes the item";
		} else if (PRIORITY.equals(key)) {
			return "P0 to P3, a manual override";
		} else if (TITLE.equals(key)) {
			return "New title";
		} else if (DUE_AT.equals(key)) {
			return "ISO-8601 due time";
		} else if (SUGGESTED.equals(key)) {
			return "false accepts an assistant suggestion";
		} else if (REASON.equals(key)) {
			return "Why, kept in the item history";
		}
		return super.getDescriptionForKey(key);
	}
}
