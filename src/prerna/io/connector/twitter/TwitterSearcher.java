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

import java.util.HashMap;
import java.util.Map;
import prerna.auth.AccessToken;
import prerna.auth.AppTokens;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.Viewpoint;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class TwitterSearcher implements IConnectorIOp {

  String url = "https://api.twitter.com/1.1/search/tweets.json";

  // name of the object to return
  String objectName = "prerna.om.Viewpoint"; // it will fill this object and return the data
  String[] beanProps = {
    "review", "author", "authorId", "location", "repeatCount", "favCount", "followerCount"
  }; // add is done when you have a list
  String jsonPattern =
      "statuses[].{review: text, author: id, authorId:user.screen_name, user_name: user.name, location:user.location, repeatCount:retweet_count, favCount: favorite_count, followerCount: user.followers_count }";

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

    AccessToken twitToken = null;
    if (user != null) {
      twitToken = user.getAccessToken(AuthProvider.TWITTER);
    }
    if (twitToken == null) {
      twitToken = AppTokens.getInstance().getAccessToken(AuthProvider.TWITTER);
    }

    if (twitToken == null) {
      SemossPixelException exception =
          new SemossPixelException(
              new NounMetadata(
                  "Requires login to twiiter",
                  PixelDataType.CONST_STRING,
                  PixelOperationType.ERROR));
      exception.setContinueThreadOfExecution(false);
      throw exception;
    }

    String accessToken = twitToken.getAccess_token();

    // make the API call
    String output = HttpHelperUtility.makeGetCall(url, accessToken, params, true);
    //		System.out.println(output);

    // need a way to convert this into a full review
    Object retObject = BeanFiller.fillFromJson(output, jsonPattern, beanProps, new Viewpoint());
    return retObject;
  }
}
