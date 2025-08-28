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
package prerna.io.connector.surveymonkey;

import java.util.HashMap;
import java.util.Map;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class MonkeyProfile implements IConnectorIOp {

  private static String url = "https://api.surveymonkey.com/v3/users/me";
  private static String[] beanProps = {"id", "email", "username", "name"};
  // need to join the first name and last name together
  private static String jsonPattern =
      "{id: id, email: email, username: username, first_name: first_name, last_name: last_name}.[id, email, username, join(' ', [first_name, last_name])]";

  @Override
  public String execute(User user, Map<String, Object> params) {
    AccessToken acToken = user.getAccessToken(AuthProvider.SURVEYMONKEY);
    return fillAccessToken(acToken, params);
  }

  public static String fillAccessToken(AccessToken acToken, Map<String, Object> params) {
    if (params == null) {
      params = new HashMap<>();
    }

    String accessToken = acToken.getAccess_token();

    // you fill what you want to send on the API call
    params.put("Bearer", accessToken);
    params.put("alt", "json");

    // make the API call
    String output = HttpHelperUtility.makeGetCall(url, accessToken, null, true);

    // fill the bean with the return
    acToken = (AccessToken) BeanFiller.fillFromJson(output, jsonPattern, beanProps, acToken);

    return output;
  }
}
