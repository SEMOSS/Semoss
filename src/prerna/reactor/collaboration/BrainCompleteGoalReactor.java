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

import prerna.collaboration.BrainAgentEdits;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class BrainCompleteGoalReactor extends AbstractCollaborationReactor {

	public BrainCompleteGoalReactor() {
		this.keysToGet = new String[] { "topicId", "goalId", "resolution" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		return mapResult(BrainAgentEdits.completeGoal(getUser(), this.insight.getRoomId(), getString("topicId"),
				getString("goalId"), getString("resolution")));
	}

	@Override
	public String getReactorDescription() {
		return "Marks an existing goal in this chat's current topics done when the conversation or sources clearly "
				+ "establish that it is achieved. Runs automatically; do not wait for the owner to ask. "
				+ "Keeps the goal text and saves the outcome. Use exact ids from Chat topics or ListTopics. "
				+ "A promise, plan, partial answer, or uncertain match is not completion. "
				+ "Tell the owner which goal was marked done only after this succeeds.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
			case "topicId" -> "Topic id from the current Chat topics or ListTopics; must apply to this chat";
			case "goalId" -> "The existing goal's noteId from this topic's open goals";
			case "resolution" -> "One short, plain-language accomplishment with the established outcome and useful facts. "
					+ "Omit source labels, citations, and internal ids, including shortened thread/message ids";
			default -> super.getDescriptionForKey(key);
		};
	}
}
