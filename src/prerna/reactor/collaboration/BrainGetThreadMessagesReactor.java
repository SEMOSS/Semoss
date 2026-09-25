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
import prerna.collaboration.BrainThreadMessages;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainGetThreadMessages(threadId=["..."], limit=[20]);
public class BrainGetThreadMessagesReactor extends AbstractCollaborationReactor {

	private static final String THREAD_ID = "threadId";
	private static final String LIMIT = "limit";

	public BrainGetThreadMessagesReactor() {
		this.keysToGet = new String[] { THREAD_ID, LIMIT };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String threadId = getString(THREAD_ID);
		if (threadId == null) {
			throw new IllegalArgumentException("Must pass a threadId");
		}
		return mapResult(BrainThreadMessages.read(user, threadId, getIntFromKeyOrCurRow(LIMIT)));
	}

	@Override
	public String getReactorDescription() {
		return "Reads a Brain thread's latest messages as clean text, fetched live; never-ingest senders are left out";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (THREAD_ID.equals(key)) {
			return "Thread id";
		}
		if (LIMIT.equals(key)) {
			return "Most recent N messages, default 20, at most 100";
		}
		return super.getDescriptionForKey(key);
	}
}
