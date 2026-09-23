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
package prerna.usertracking.reactors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.UserCatalogVoteUtils;
import prerna.util.Utility;

public class GetUserEngineVotesReactor extends AbstractReactor {

	public GetUserEngineVotesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		if (Utility.isUserTrackingDisabled()) {
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.USER_TRACKING_DISABLED);
		}

		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Engine Id cannot be null.");
		}

		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Engine cannot be viewed by user.");
		}

		// get the primary login in case of different upvote and downvote for different
		// authproviders.
		List<Pair<String, String>> creds = User.getUserIdAndType(this.insight.getUser());
		Pair<String, String> primaryCredentials = creds.get(0);

		Map<Pair<String, String>, Integer> userVotes = UserCatalogVoteUtils.getVote(creds, engineId);

		int userVote = 0;
		if (userVotes.containsKey(primaryCredentials)) {
			userVote = userVotes.get(primaryCredentials);
		}

		int total = UserCatalogVoteUtils.getAllVotes(engineId);

		Map<String, Integer> votes = new HashMap<>();
		votes.put("userVote", userVote);
		votes.put("total", total);

		return new NounMetadata(votes, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Returns the current user's vote and the total aggregated vote count for an engine.";
	}

}