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
package prerna.io.connector.twitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.TwitterTweet;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class TwitterSearcher implements IConnectorIOp {

	String url = "https://api.twitter.com/1.1/search/tweets.json";

	String jsonPattern = "statuses[].{review: text, author: id, authorId:user.screen_name, user_name: user.name, location:user.location, repeatCount:retweet_count, favCount: favorite_count, followerCount: user.followers_count }";

	// things you can feed into a search
	// https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets
	// q - query string
	// geocode - lat, long, 2mile
	// result_type - mixed, recent, popular
	// count - how many to get - defaults to 10
	// max_id, since_id - low level control in terms of the id to get

	@Override
	public Object execute(User user, Map<String, Object> params) {
		if (params == null) {
			params = new HashMap<>();
		}

		String accessToken = IConnectorIOp.requireAccessToken(user, AuthProvider.TWITTER).getAccess_token();

		// make the API call
		String output = HttpHelperUtility.makeGetCall(url, accessToken, params, true);
//		System.out.println(output);

		List<JsonNode> nodes = BeanFiller.getJmesObjects(output, jsonPattern);
		if (nodes.isEmpty()) {
			return new TwitterTweet(null, null, null, null, 0, 0, 0);
		}
		List<TwitterTweet> tweets = new ArrayList<>();
		for (JsonNode node : nodes) {
			tweets.add(new TwitterTweet(BeanFiller.getText(node, "review"), BeanFiller.getText(node, "author"),
					BeanFiller.getText(node, "authorId"), BeanFiller.getText(node, "location"),
					BeanFiller.getInt(node, "repeatCount"), BeanFiller.getInt(node, "favCount"),
					BeanFiller.getInt(node, "followerCount")));
		}
		return tweets;
	}
}
