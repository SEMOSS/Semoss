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

import prerna.collaboration.BrainTopicMatches;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainPreviewTopicMatches(topicId=["..."], limit=[200]); poll with BrainGetJob(jobId=["..."]), apply with
// BrainApplyTopicMatches
public class BrainPreviewTopicMatchesReactor extends AbstractCollaborationReactor {

	private static final String TOPIC_ID = "topicId";
	private static final String LIMIT = "limit";

	public BrainPreviewTopicMatchesReactor() {
		this.keysToGet = new String[] { TOPIC_ID, LIMIT };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(BrainTopicMatches.start(getUser(), getString(TOPIC_ID), getIntFromKeyOrCurRow(LIMIT)));
	}

	@Override
	public String getReactorDescription() {
		return "Starts a background job that scores the owner's newest conversations against their topics without "
				+ "writing anything and lists the ones that fit this topic; when done, counts.matches holds rows with "
				+ "threadId, subject, confidence, fit (best or shared), links, ops and a suggested op";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (TOPIC_ID.equals(key)) {
			return "The active or dormant topic that was added or edited";
		} else if (LIMIT.equals(key)) {
			return "How many of the newest eligible conversations to score, 1 to 1000; default 200";
		}
		return super.getDescriptionForKey(key);
	}
}
