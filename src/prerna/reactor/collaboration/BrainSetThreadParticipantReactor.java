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
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainSetThreadParticipant(threadId=["..."], personId=["..."], included=[false]);
public class BrainSetThreadParticipantReactor extends AbstractCollaborationReactor {

	private static final String THREAD_ID = "threadId";
	private static final String PERSON_ID = "personId";
	private static final String INCLUDED = "included";

	public BrainSetThreadParticipantReactor() {
		this.keysToGet = new String[] { THREAD_ID, PERSON_ID, INCLUDED };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String threadId = getString(THREAD_ID);
		String personId = getString(PERSON_ID);
		Boolean included = getBoolean(INCLUDED);
		if (threadId == null || personId == null || included == null) {
			throw new IllegalArgumentException("Must pass a threadId, personId, and included");
		}
		return mapResult(BrainThreadUtils.setThreadParticipant(user, threadId, personId, included));
	}

	@Override
	public String getReactorDescription() {
		return "Excludes or includes a person on one Brain thread; an excluded person's messages make no items or alerts";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (THREAD_ID.equals(key)) {
			return "Thread id";
		} else if (PERSON_ID.equals(key)) {
			return "Person id of a participant on the thread";
		} else if (INCLUDED.equals(key)) {
			return "false excludes the person on this thread, true includes them again";
		}
		return super.getDescriptionForKey(key);
	}
}
