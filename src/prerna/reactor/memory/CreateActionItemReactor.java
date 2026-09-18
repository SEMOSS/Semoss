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
package prerna.reactor.memory;

import java.sql.Timestamp;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.MemoryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Creates a first-class action-item memory, optionally linked to a parent
 * memory. Same workspace-visibility check as AddMemoryReactor.
 */
public class CreateActionItemReactor extends AbstractReactor {

	public CreateActionItemReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CONTENT.getKey(), ReactorKeysEnum.MEMORY_ID.getKey(),
				ReactorKeysEnum.OWNER.getKey(), ReactorKeysEnum.DUE_DATE.getKey(), ReactorKeysEnum.STATUS.getKey(),
				ReactorKeysEnum.ROOM_ID.getKey(), ReactorKeysEnum.WORKSPACE_ID.getKey(),
				ReactorKeysEnum.METADATA.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0, 0, 0 };
	}

	@Override
	public String getReactorDescription() {
		return """
				Creates a first-class action-item memory (a task to follow up on), \
				optionally linked to a parent memory it was derived from, with an owner, \
				due date, and status.\
				""";
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new IllegalArgumentException("You are not properly logged in");
		}
		String userId = user.getPrimaryLoginToken().getId();

		String content = this.keyValue.get(ReactorKeysEnum.CONTENT.getKey());
		String parentMemoryId = this.keyValue.get(ReactorKeysEnum.MEMORY_ID.getKey());
		String owner = this.keyValue.get(ReactorKeysEnum.OWNER.getKey());
		String status = this.keyValue.get(ReactorKeysEnum.STATUS.getKey());
		String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());
		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());
		if (workspaceId != null && !workspaceId.isBlank()
				&& !SecurityProjectUtils.userCanViewProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have access to the workspace");
		}

		String dueDateStr = this.keyValue.get(ReactorKeysEnum.DUE_DATE.getKey());
		Timestamp dueDate = null;
		if (dueDateStr != null && !dueDateStr.isBlank()) {
			try {
				dueDate = Timestamp.valueOf(dueDateStr);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("dueDate must be a timestamp like 'yyyy-MM-dd HH:mm:ss'");
			}
		}
		Map<String, Object> metadata = getMapFromKeyOrCurRow(ReactorKeysEnum.METADATA.getKey());

		String actionItemId = MemoryUtils.createActionItem(userId, content, parentMemoryId, owner, dueDate, status,
				roomId, workspaceId, metadata);
		return new NounMetadata(actionItemId, PixelDataType.CONST_STRING);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.CONTENT.getKey())) {
			return "The action item text";
		}
		if (key.equals(ReactorKeysEnum.MEMORY_ID.getKey())) {
			return "Optional parent memory this action item was derived from";
		}
		if (key.equals(ReactorKeysEnum.OWNER.getKey())) {
			return "Optional owner/assignee";
		}
		if (key.equals(ReactorKeysEnum.DUE_DATE.getKey())) {
			return "Optional due date, e.g. '2026-01-01 00:00:00'";
		}
		if (key.equals(ReactorKeysEnum.STATUS.getKey())) {
			return "Status: open/in_progress/blocked/completed/cancelled (defaults to open)";
		}
		return super.getDescriptionForKey(key);
	}

}
