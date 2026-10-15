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

/** Save the editable draft against the last observed revision. */
public class BrainSaveTopicReviewReactor extends AbstractCollaborationReactor {
	public BrainSaveTopicReviewReactor() {
		this.keysToGet = new String[] { "reviewId", "revision", "draft" };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		var user = getUser();
		Integer revision = getIntFromKeyOrCurRow("revision");
		Map<String, Object> draft = getMapFromKeyOrCurRow("draft");
		if (revision == null || revision < 1 || draft == null) {
			throw new IllegalArgumentException("Pass a saved review ID, positive revision and draft map");
		}
		return mapResult(BrainTopicReviewUtils.save(user, getString("reviewId"), revision, draft));
	}

	@Override
	public String getReactorDescription() {
		return "Saves an owner-scoped topic-review draft with revision checking; no topic profiles or thread assignments are applied";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
		case "reviewId" -> "The review ID returned by BrainStartTopicReview";
		case "revision" -> "Last observed saved draft revision; conflicting edits are rejected";
		case "draft" -> "topics list (maximum 100): key, id, name, description, short (empty uses name), keep and removedPeople. Source evidence is server-owned";
		default -> super.getDescriptionForKey(key);
		};
	}
}
