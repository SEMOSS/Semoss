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

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.MemoryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Shares (or unshares) a personal memory to a workspace. Promoting content into
 * a workspace requires edit access on that workspace - a stricter check than
 * the view-only check used to read/list workspace memories - since this action
 * injects new shared content other workspace members will see.
 */
public class PromoteMemoryToWorkspaceReactor extends AbstractReactor {

	public PromoteMemoryToWorkspaceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.MEMORY_ID.getKey(), ReactorKeysEnum.WORKSPACE_ID.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public String getReactorDescription() {
		return """
				Shares (or unshares) a personal memory to a workspace so everyone with \
				access to that workspace can see it. Only the memory's creator may promote \
				it, and the caller must have edit access to the target workspace.\
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

		String memoryId = this.keyValue.get(ReactorKeysEnum.MEMORY_ID.getKey());
		String workspaceId = this.keyValue.get(ReactorKeysEnum.WORKSPACE_ID.getKey());

		if (workspaceId != null && !workspaceId.isBlank()
				&& !SecurityProjectUtils.userCanEditProject(user, workspaceId)) {
			throw new IllegalArgumentException(
					"Workspace " + workspaceId + " does not exist or user does not have edit access to the workspace");
		}

		boolean updated = MemoryUtils.setMemoryWorkspace(memoryId, userId, workspaceId);
		return new NounMetadata(updated, PixelDataType.BOOLEAN);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.MEMORY_ID.getKey())) {
			return "Id of the memory to share/unshare";
		}
		if (key.equals(ReactorKeysEnum.WORKSPACE_ID.getKey())) {
			return "Workspace to share the memory with, or omit to make the memory personal again";
		}
		return super.getDescriptionForKey(key);
	}

}
