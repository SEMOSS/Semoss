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
package prerna.collaboration;

import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.IAgentRunHook;

/**
 * Added by AgentConfigLoader to every root run in an assistant room: once the run finishes, it asks for a
 * review of the chat (BrainMemoryReview), which waits for the chat to go quiet first, and for a topic check of
 * the chat (BrainChatTopics).
 */
public final class CollaborationMemoryRunHook implements IAgentRunHook {

	@Override
	public void afterRun(AgentRunContext ctx, AgentHarnessResult result) {
		if (result == null || result.getCompletionError() != null
				|| ctx.getSpawnDepth() != AgentRunContext.ROOT_SPAWN_DEPTH) {
			return;
		}
		Room room = ctx.getRoom();
		User user = ctx.getInsight() == null ? null : ctx.getInsight().getUser();
		if (user != null && CollaborationUtils.isAssistantRoom(room)) {
			BrainMemoryReview.schedule(user, room.getId(), CollaborationUtils.threadIdOf(room));
			BrainChatTopics.schedule(user, room.getId());
		}
	}
}
