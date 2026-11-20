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

import prerna.collaboration.WorkTwinFiles;
import prerna.engine.impl.model.RoomUtils;
import prerna.reactor.ReactorDefinition;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// WorkSaveToTwin(path=["notes/about-me.md"]);
// takes no twin on purpose: the twin is the agent of the chat it runs in, so a chat adds only to its own
public class WorkSaveToTwinReactor extends AbstractCollaborationReactor {

	private static final String PATH = "path";

	public WorkSaveToTwinReactor() {
		this.keysToGet = new String[] { PATH };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		String roomId = this.insight.getRoomId();
		if (roomId == null) {
			throw new IllegalArgumentException("SaveToDigitalTwin only works inside a chat");
		}
		return mapResult(WorkTwinFiles.add(getUser(), RoomUtils.getOrLoadRoom(roomId, this.insight), getString(PATH)));
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// everyone the twin is shared with can read what is added, so it waits for the
		// owner's approval
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Adds a file from this chat's folder to the files of the Digital Twin this chat is with, where "
						+ "every chat with the twin reads it and everyone the twin is shared with can read it. Only in "
						+ "the chat the twin's owner has with their own twin, and only when they ask for it. The file "
						+ "is one the owner put into this chat, or a note written into the chat's folder first. A name "
						+ "the twin already has gets a numbered copy; nothing is replaced")
				.key(PATH, "The file, relative to this chat's folder, such as notes/about-me.md")
				.output(Map.class, "file, where the copy is among the twin's files, as an agent:/ path");
	}
}
