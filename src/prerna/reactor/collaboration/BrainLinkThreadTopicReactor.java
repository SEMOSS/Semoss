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
import prerna.collaboration.BrainThreadUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainLinkThreadTopic(threadId=["..."], topicId=["..."], primary=[true], remove=[false]);
public class BrainLinkThreadTopicReactor extends AbstractCollaborationReactor {

	private static final String THREAD_ID = "threadId";
	private static final String TOPIC_ID = "topicId";
	private static final String PRIMARY = "primary";
	private static final String REMOVE = "remove";

	public BrainLinkThreadTopicReactor() {
		this.keysToGet = new String[] { THREAD_ID, TOPIC_ID, PRIMARY, REMOVE };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String threadId = getString(THREAD_ID);
		String topicId = getString(TOPIC_ID);
		if (threadId == null || topicId == null) {
			throw new IllegalArgumentException("Must pass a threadId and topicId");
		}
		return new NounMetadata(BrainThreadUtils.linkThreadTopic(user, threadId, topicId,
				Boolean.TRUE.equals(getBoolean(PRIMARY)), Boolean.TRUE.equals(getBoolean(REMOVE))),
				PixelDataType.VECTOR, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Links or unlinks a topic on a Brain thread; keeps exactly one primary link";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (THREAD_ID.equals(key)) {
			return "Thread id";
		} else if (TOPIC_ID.equals(key)) {
			return "Topic id";
		} else if (PRIMARY.equals(key)) {
			return "true makes this the thread's one primary topic";
		} else if (REMOVE.equals(key)) {
			return "true unlinks the topic instead";
		}
		return super.getDescriptionForKey(key);
	}
}
