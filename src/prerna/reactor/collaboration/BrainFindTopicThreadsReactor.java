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

import prerna.collaboration.BrainTopicReviewUtils;
import prerna.reactor.ReactorDefinition;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import java.util.Map;

/**
 * One onboarding chat turn over the topic draft. Read-only; the owner applies
 * proposed changes through the draft.
 */
public class BrainFindTopicThreadsReactor extends AbstractCollaborationReactor {
	public BrainFindTopicThreadsReactor() {
		this.keysToGet = new String[] { "reviewId", "revision", "topic" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		Integer revision = getIntFromKeyOrCurRow("revision");
		var topic = getMapFromKeyOrCurRow("topic");
		if (revision == null || topic == null) throw new IllegalArgumentException("Pass the saved review revision and the topic");
		return mapResult(BrainTopicReviewUtils.findThreads(getUser(), getString("reviewId"), revision, topic));
	}

	@Override
	public ReactorDefinition getReactorDefinition() {
		return super.getReactorDefinition().description(
				"Onboarding topic builder: finds the conversations that fit a topic being set up (from its people, name, clues and the suggestions it draws on), judges each, and returns the best with a reason and whether it is a clear fit; conversations other built topics kept are left out; no writes")
				.key("reviewId", "The id of the signed-in user's topic review.")
				.key("revision", key -> key.type(Integer.class).description(
						"The review revision the caller last read. The call fails when the saved review has changed since."))
				.key("topic", key -> key.type(Map.class).description(
						"The topic being set up: name, description, clues (list), people (person ids), suggestionKeys (list), topicKey when rebuilding one, exclude (thread ids to leave out)."));
	}
}
