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

import prerna.auth.User;
import prerna.collaboration.BrainMemoryUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainDeleteMemory(memoryId=["..."]); or BrainDeleteMemory(all=[true]);
public class BrainDeleteMemoryReactor extends AbstractCollaborationReactor {

	private static final String MEMORY_ID = "memoryId";
	private static final String ALL = "all";

	public BrainDeleteMemoryReactor() {
		this.keysToGet = new String[] { MEMORY_ID, ALL };
		this.keyRequired = new int[] { 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		if (Boolean.TRUE.equals(getBoolean(ALL))) {
			return mapResult(BrainMemoryUtils.deleteAllMemories(user));
		}
		String memoryId = getString(MEMORY_ID);
		if (memoryId == null) {
			throw new IllegalArgumentException("Must pass a memoryId, or all=true");
		}
		return mapResult(BrainMemoryUtils.deleteMemory(user, memoryId));
	}

	@Override
	public String getReactorDescription() {
		return "Erases one Brain memory, or all of them";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (MEMORY_ID.equals(key)) {
			return "Memory id";
		} else if (ALL.equals(key)) {
			return "true to erase every memory and suggestion";
		}
		return super.getDescriptionForKey(key);
	}
}
