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
import java.util.List;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.RemoteItem;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GoogleListFiles implements IConnectorIOp {

	// lists the various files for this user
	// if the
	// name of the object to return
	String objectName = "prerna.om.RemoteItem"; // it will fill this object and return the data
	String[] beanProps = { "id", "name", "type" }; // add is done when you have a list
	String jsonPattern = "files[].{id:id, name:name, type:mimeType}";

	@Override
	public Object execute(User user, Map<String, Object> params) {
		if (params == null) {
			params = new HashMap<>();
		}

		// TODO Auto-generated method stub
		AccessToken googToken = user.getAccessToken(AuthProvider.GOOGLE);
		String accessToken = googToken.getAccess_token();

		// you fill what you want to send on the API call
		params.put("access_token", accessToken);

		String url = "https://www.googleapis.com/drive/v3/files";
		String output = HttpHelperUtility.makeGetCall(url, accessToken, params, false);

		// fill the bean with the return
		List<RemoteItem> fileList = (List) BeanFiller.fillFromJson(output, jsonPattern, beanProps, new RemoteItem());
		return fileList;
	}

}
