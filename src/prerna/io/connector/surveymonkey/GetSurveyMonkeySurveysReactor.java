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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.om.RemoteItem;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GetSurveyMonkeySurveysReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		String url_str = "https://api.surveymonkey.com/v3/surveys";
		List<Map<String, Object>> masterList = new ArrayList<>();

		String[] beanProps = { "id", "name" };
		String jsonPattern = "data[].{id: id, name: title}";

		// get access token
		String accessToken = null;
		User user = this.insight.getUser();
		try {
			if (user == null || user.getAccessToken(AuthProvider.SURVEYMONKEY) == null) {
				Map<String, Object> retMap = new HashMap<>();
				retMap.put("type", "surveymonkey");
				retMap.put("message", "Please login to your Survey Monkey account");
				throwLoginError(retMap);
			} else {
				AccessToken token = user.getAccessToken(AuthProvider.SURVEYMONKEY);
				accessToken = token.getAccess_token();
			}
		} catch (Exception e) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("type", "surveymonkey");
			retMap.put("message", "Please login to your Survey Monkey account");
			throwLoginError(retMap);
		}

		// query params
		Map<String, Object> params = new HashMap<>();
		params.put("per_page", 1000);
		params.put("sort_order", "DESC");
		// make the call
		String output = HttpHelperUtility.makeGetCall(url_str, accessToken, params, true);

		// fill the bean with the return
		Object C = BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		if (C instanceof RemoteItem) {
			RemoteItem fileList = (RemoteItem) C;
			HashMap<String, Object> tempMap = new HashMap<>();
			tempMap.put("name", fileList.getName());
			tempMap.put("id", fileList.getId());
			masterList.add(tempMap);
		} else {
			List<RemoteItem> fileList = (List<RemoteItem>) C;
			for (RemoteItem entry : fileList) {
				HashMap<String, Object> tempMap = new HashMap<>();
				tempMap.put("name", entry.getName());
				tempMap.put("id", entry.getId());
				masterList.add(tempMap);
			}
		}

		return new NounMetadata(masterList, PixelDataType.MAP);
	}
}
