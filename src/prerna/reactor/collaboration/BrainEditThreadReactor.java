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
import prerna.collaboration.BrainAgentEdits;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainEditThread(threadId=["..."], removeTopic=["Air Force AIMES"], addTopic=["Cloud Infrastructure"]);
public class BrainEditThreadReactor extends AbstractCollaborationReactor {

	private static final String THREAD_ID = "threadId";
	private static final String ADD_TOPIC = "addTopic";
	private static final String REMOVE_TOPIC = "removeTopic";
	private static final String MAKE_PRIMARY = "makePrimary";
	private static final String MUTED = "muted";
	private static final String NOT_AUTOMATED = "notAutomated";

	public BrainEditThreadReactor() {
		this.keysToGet = new String[] { THREAD_ID, ADD_TOPIC, REMOVE_TOPIC, MAKE_PRIMARY, MUTED, NOT_AUTOMATED };
		this.keyRequired = new int[] { 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		return mapResult(BrainAgentEdits.editThread(user, this.insight, getString(THREAD_ID), getString(ADD_TOPIC),
				getString(REMOVE_TOPIC), getBoolean(MAKE_PRIMARY), getBoolean(MUTED), getBoolean(NOT_AUTOMATED)));
	}

	@Override
	public Map<String, String> getMcpToolMetadata() {
		// changes the owner's Brain, so it waits for their approval
		Map<String, String> meta = super.getMcpToolMetadata();
		meta.put(MCPUtility.SMSS_MCP_EXECUTION, MCPUtility.MCPExecution.ASK.getValue());
		return meta;
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (MAKE_PRIMARY.equals(key) || MUTED.equals(key) || NOT_AUTOMATED.equals(key)) {
			return MCP_KEY_TYPE.BOOLEAN;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return "Changes how one thread sits in the owner's Brain, as the owner would on the thread: tag it with a "
				+ "topic, untag it from a topic, mute or unmute it, or correct it as not automated. Several changes "
				+ "can go in one call. It waits for the owner to approve. Take the threadId from SearchMail "
				+ "or the current thread";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (THREAD_ID.equals(key)) {
			return "Thread id";
		} else if (ADD_TOPIC.equals(key)) {
			return "Topic to tag the thread with: its id or its name";
		} else if (REMOVE_TOPIC.equals(key)) {
			return "Topic to untag the thread from: its id or its name; the thread is not deleted";
		} else if (MAKE_PRIMARY.equals(key)) {
			return "true makes addTopic the thread's one main topic";
		} else if (MUTED.equals(key)) {
			return "true mutes the thread so Brain leaves it out, false unmutes it";
		} else if (NOT_AUTOMATED.equals(key)) {
			return "true says the thread is not automated mail and keeps it that way, false removes the correction";
		}
		return super.getDescriptionForKey(key);
	}
}
