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
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Apply a saved revision atomically and start/resume its own filing job. */
public class BrainApplyTopicReviewReactor extends AbstractCollaborationReactor {
	public BrainApplyTopicReviewReactor() {
		this.keysToGet = new String[] { "reviewId", "revision", "retryFiling" };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		var user = getUser();
		Integer revision = getIntFromKeyOrCurRow("revision");
		if (revision == null || revision < 1) {
			throw new IllegalArgumentException("Pass a saved review ID and positive revision");
		}
		return mapResult(BrainTopicReviewUtils.apply(user, getString("reviewId"), revision,
				Boolean.TRUE.equals(getBoolean("retryFiling"))));
	}

	@Override
	public String getReactorDescription() {
		return "Atomically applies a saved topic-review revision, returning saved names/descriptions/IDs and its exact filing job. Repeating the same revision does not recreate topics. Zero kept topics require no filing job";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		return switch (key) {
		case "reviewId" -> "The owner's review ID";
		case "revision" -> "Saved draft revision to apply; stale revisions are rejected";
		case "retryFiling" -> "true to retry failed or partially completed filing without recreating topics; default false";
		default -> super.getDescriptionForKey(key);
		};
	}
}
