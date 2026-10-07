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
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainForget(memoryId=["..."]);
// The thread assistant's Forget tool: hides a memory it saved; one the owner wrote waits for them
public class BrainForgetReactor extends AbstractCollaborationReactor {

	private static final String MEMORY_ID = "memoryId";

	public BrainForgetReactor() {
		this.keysToGet = new String[] { MEMORY_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		BrainMemoryUtils.requireAssistantMemory(user);
		String memoryId = BrainMemoryUtils.memoryIdOf(getString(MEMORY_ID));
		if (memoryId == null) {
			throw new IllegalArgumentException("Must pass the memoryId to forget");
		}
		return mapResult(BrainMemoryUtils.forget(user, memoryId));
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.UI_COMPONENT, MCPUtility.COMPONENT_MEMORY);
		return meta;
	}

	@Override
	public String getReactorDescription() {
		return "Drop a memory when the owner asks you to forget it or says it is wrong and gives nothing in its place. "
				+ "A memory the owner wrote or confirmed is kept until they approve it in the chat.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (MEMORY_ID.equals(key)) {
			return "Id of the memory, as in [m:id] under What you remember or in SearchMemories results";
		}
		return super.getDescriptionForKey(key);
	}
}
