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

import java.util.Map;

import prerna.auth.User;
import prerna.collaboration.BrainMemoryUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainSaveMemory(memory=[{"id": "...", "kind": "fact", "text": "...", "about": [{"type": "person", "id": "..."}]}]);
public class BrainSaveMemoryReactor extends AbstractCollaborationReactor {

	private static final String MEMORY = "memory";

	public BrainSaveMemoryReactor() {
		this.keysToGet = new String[] { MEMORY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		Map<String, Object> memory = getMapFromKeyOrCurRow(MEMORY);
		if (memory == null) {
			throw new IllegalArgumentException("Must pass a memory map");
		}
		return mapResult(BrainMemoryUtils.saveMemory(user, memory));
	}

	@Override
	public String getReactorDescription() {
		return "Adds a Brain memory, or changes the keys passed on one; the owner's save confirms it";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (MEMORY.equals(key)) {
			return "Memory map: id (omit to create), kind (preference or fact), text, about [{type, id}], pinned, "
					+ "expiresAt";
		}
		return super.getDescriptionForKey(key);
	}
}
