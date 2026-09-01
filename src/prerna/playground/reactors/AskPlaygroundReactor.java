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
package prerna.playground.reactors;

import java.util.concurrent.CompletableFuture;

import prerna.engine.impl.model.Room;
import prerna.om.ThreadStore;
import prerna.playground.PlaygroundUtils;
import prerna.reactor.agent.run.AgentRoomNamer;
import prerna.reactor.model.AskRoomReactor;
import prerna.sablecc2.comm.JobStreamEnvelopes;
import prerna.theme.PlaygroundThemeUtils;

public class AskPlaygroundReactor extends AskRoomReactor {

	@Override
	protected String getProjectIdOverride() {
		return PlaygroundUtils.PLAYGROUND_PROJECT_ID;
	}

	@Override
	protected boolean shouldHideSystemMessages() {
		return PlaygroundThemeUtils.hidePlaygroundSystemMessages();
	}

	@Override
	protected CompletableFuture<String> beforeRoomAsk(Room room, String question, String engineId) {
		String jobId = ThreadStore.getJobId();
		String defaultName = question == null ? null : question.substring(0, Math.min(question.length(), 100));
		CompletableFuture<String> roomNameFuture = AgentRoomNamer.nameRoomAsync(room.getId(), question, engineId,
				room.getUserId(), this.insight);
		roomNameFuture.thenAccept(roomName -> {
			if (roomName != null && !roomName.equals(defaultName)) {
				JobStreamEnvelopes.roomName(jobId, roomName);
			}
		});
		return roomNameFuture;
	}

	@Override
	public String getReactorDescription() {
		return "This method is used to run an LLM text-generation call (Playground) returns both input and response message objects.";
	}
}
