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
package prerna.io.connector.google;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.EntityResolution;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GoogleEntityResolver implements IConnectorIOp {

	String[] beanProps = { "entity_name", "entity_type", "wiki_url", "content", "content_subtype" };
	String jsonPattern = "entities[].{entity_name : name, entity_type : type, wiki_url : metadata.wikipedia_url, content : mentions[].text.content, content_subtype : mentions[].type}";

	@Override
	public Object execute(User user, Map<String, Object> params) {
		// if no input, unsure what you will get...
		if (params == null) {
			params = new HashMap<>();
		}
		AccessToken googToken = user.getAccessToken(AuthProvider.GOOGLE);
		String accessToken = googToken.getAccess_token();

		// make the API call
		String url = "https://language.googleapis.com/v1/documents:analyzeEntities";
		String jsonString = HttpHelperUtility.makePostCall(url, accessToken, params, true);

		EntityResolution entity = new EntityResolution();
		// fill the bean with the return
		Object returnObj = BeanFiller.fillFromJson(jsonString, jsonPattern, beanProps, entity);
		return returnObj;
	}

}
