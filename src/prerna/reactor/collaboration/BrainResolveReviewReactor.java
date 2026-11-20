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

import prerna.auth.User;
import prerna.collaboration.BrainReviewUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

// BrainResolveReview(reviewId=["..."], action=["Accept"], paramValues=[{...}]);
public class BrainResolveReviewReactor extends AbstractCollaborationReactor {

	private static final String REVIEW_ID = "reviewId";
	private static final String ACTION = "action";
	private static final String PARAM_VALUES = "paramValues";

	public BrainResolveReviewReactor() {
		this.keysToGet = new String[] { REVIEW_ID, ACTION, PARAM_VALUES };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		User user = getUser();
		String reviewId = getString(REVIEW_ID);
		if (reviewId == null) {
			throw new IllegalArgumentException("Must pass a reviewId");
		}
		Map<String, Object> paramValues = getGenRowStruct(PARAM_VALUES) == null ? null
				: getMapFromKeyOrCurRow(PARAM_VALUES);
		return mapResult(BrainReviewUtils.resolveReview(user, reviewId, getString(ACTION), paramValues));
	}

	@Override
	public String getReactorDescription() {
		return "Records the answer to a Brain review question";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (REVIEW_ID.equals(key)) {
			return "Review id";
		} else if (ACTION.equals(key)) {
			return "One of the entry's actions, or accept, dismiss, both, choose, merge";
		} else if (PARAM_VALUES.equals(key)) {
			return "Map with what the answer needs, e.g. topicId";
		}
		return super.getDescriptionForKey(key);
	}
}
