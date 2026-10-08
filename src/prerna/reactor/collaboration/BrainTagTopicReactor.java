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
import prerna.collaboration.BrainTopicRoomUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainTagTopic(topic=["..."], confident=[true]);
// The owner's assistant tags the chat it is in with a topic; no approval, the owner can undo or dismiss it
public class BrainTagTopicReactor extends AbstractCollaborationReactor {

	private static final String TOPIC = "topic";
	private static final String CONFIDENT = "confident";

	public BrainTagTopicReactor() {
		this.keysToGet = new String[] { TOPIC, CONFIDENT };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String roomId = this.insight.getRoomId();
		if (roomId == null) {
			throw new IllegalArgumentException("TagTopic only works inside a chat");
		}
		return mapResult(BrainTopicRoomUtils.tagRoomTopic(user, roomId, getString(TOPIC),
				Boolean.TRUE.equals(getBoolean(CONFIDENT))));
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (CONFIDENT.equals(key)) {
			return MCP_KEY_TYPE.BOOLEAN;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return "Tags this chat with one of the owner's topics, so that topic's notes, goals and context follow the "
				+ "chat. Use it when the conversation is about a topic the chat does not have yet. With confident "
				+ "true (the chat is plainly about that topic) it applies right away and the owner can undo it; "
				+ "otherwise it is a suggestion the owner accepts or dismisses. A topic the owner removed from this "
				+ "chat is never tagged again. Take topic ids from ListTopics";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (TOPIC.equals(key)) {
			return "The topic's id or name";
		} else if (CONFIDENT.equals(key)) {
			return "true when the chat is plainly about this topic; false or left out makes it a suggestion";
		}
		return super.getDescriptionForKey(key);
	}
}
