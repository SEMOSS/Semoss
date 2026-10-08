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

import java.util.Map;

import prerna.collaboration.BrainTopicReviewUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Apply a typed, reversible change to the draft only. */
public class BrainChangeTopicReviewReactor extends AbstractCollaborationReactor {
	public BrainChangeTopicReviewReactor() {
		this.keysToGet = new String[] { "reviewId", "revision", "operationId", "change" };
		this.keyRequired = new int[] { 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		var user = getUser();
		Integer revision = getIntFromKeyOrCurRow("revision");
		Map<String, Object> change = getMapFromKeyOrCurRow("change");
		if (revision == null || change == null) {
			throw new IllegalArgumentException("Pass a saved review revision and a change map");
		}
		return mapResult(BrainTopicReviewUtils.change(user, getString("reviewId"), revision,
				getString("operationId"), change));
	}

	@Override
	public String getReactorDescription() {
		return "Revision-checked, idempotent topic-review draft correction. Changes: confirm/reject/move/also_link with topicKey, threadIds, preview versions and targetKey where needed; organize with reviewed groups and scopeVersion; undo with changeId. Real links and combinations are applied only by BrainApplyTopicReview";
	}
}
