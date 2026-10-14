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

// BrainResolveMemory(memoryId=["..."], action=["accept"]);
public class BrainResolveMemoryReactor extends AbstractCollaborationReactor {

	private static final String MEMORY_ID = "memoryId";
	private static final String ACTION = "action";

	public BrainResolveMemoryReactor() {
		this.keysToGet = new String[] { MEMORY_ID, ACTION };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String memoryId = getString(MEMORY_ID);
		if (memoryId == null) {
			throw new IllegalArgumentException("Must pass a memoryId");
		}
		return mapResult(BrainMemoryUtils.resolveMemory(user, memoryId, getString(ACTION)));
	}

	@Override
	public String getReactorDescription() {
		return "Accepts, confirms, dismisses, restores, reopens, or unconfirms a Brain memory; returns { memory, restored }";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (MEMORY_ID.equals(key)) {
			return "Memory id";
		} else if (ACTION.equals(key)) {
			return "accept (a suggestion), confirm (a learned memory), dismiss (a suggestion or learned memory; puts "
					+ "back what it replaced), restore (a dismissed memory, as active), reopen (a dismissed or accepted "
					+ "suggestion), unconfirm (a confirmed memory the assistant saved)";
		}
		return super.getDescriptionForKey(key);
	}
}
