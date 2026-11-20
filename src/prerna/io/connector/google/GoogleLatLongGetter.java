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
import prerna.auth.AppTokens;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.io.connector.IConnectorIOp;
import prerna.om.GeoLocation;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.util.BeanFiller;

public class GoogleLatLongGetter implements IConnectorIOp {

	// name of the object to return
	String[] beanProps = { "latitude", "longitude" }; // add is done when you have a list
	String jsonPattern = "results[*].geometry.location.[lat, lng][]";

	@Override
	public Object execute(User user, Map params) {
		if (params == null) {
			params = new HashMap<>();
		}

		AccessToken googToken = null;
		if (user != null) {
			googToken = user.getAccessToken(AuthProvider.GOOGLE_MAP);
		}
		if (googToken == null) {
			googToken = AppTokens.getInstance().getAccessToken(AuthProvider.GOOGLE_MAP);
		}

		if (googToken == null) {
			SemossPixelException exception = new SemossPixelException(
					new NounMetadata("Requires login to google", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		String accessToken = googToken.getAccess_token();
		// you fill what you want to send on the API call
		// the other thing it needs is an address
		params.put("key", accessToken);

		// make the API call
		String url = "https://maps.googleapis.com/maps/api/geocode/json";
		String output = HttpHelperUtility.makeGetCall(url, accessToken, params, false);

		// fill the bean with the return
		GeoLocation retLocation = (GeoLocation) BeanFiller.fillFromJson(output, jsonPattern, beanProps,
				new GeoLocation());
		return retLocation;
	}

}
