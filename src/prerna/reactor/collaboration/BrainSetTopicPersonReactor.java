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
import prerna.collaboration.BrainTopicUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainSetTopicPerson(topicId=["..."], personId=["..."], state=["member"], role=["Google lead"]);
public class BrainSetTopicPersonReactor extends AbstractCollaborationReactor {

	private static final String TOPIC_ID = "topicId";
	private static final String PERSON_ID = "personId";
	private static final String STATE = "state";
	private static final String ROLE = "role";

	public BrainSetTopicPersonReactor() {
		this.keysToGet = new String[] { TOPIC_ID, PERSON_ID, STATE, ROLE };
		this.keyRequired = new int[] { 1, 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String topicId = getString(TOPIC_ID);
		String personId = getString(PERSON_ID);
		if (topicId == null || personId == null) {
			throw new IllegalArgumentException("Must pass a topicId and personId");
		}
		return mapResult(BrainTopicUtils.setTopicPerson(user, topicId, personId, getString(STATE), getString(ROLE)));
	}

	@Override
	public String getReactorDescription() {
		return "Adds, accepts, or removes a person on a Brain topic";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (TOPIC_ID.equals(key)) {
			return "Topic id";
		} else if (PERSON_ID.equals(key)) {
			return "Person id";
		} else if (STATE.equals(key)) {
			return "member, suggested, or removed; removing stops the person as a signal but keeps thread links";
		} else if (ROLE.equals(key)) {
			return "Role in the topic; defaults to the person's job title for a new member";
		}
		return super.getDescriptionForKey(key);
	}
}
