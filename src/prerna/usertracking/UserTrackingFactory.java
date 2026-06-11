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
package prerna.usertracking;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.usertracking.geoip2.Geoip2UserTrackingUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public class UserTrackingFactory {

	private static final Logger classLogger = LogManager.getLogger(UserTrackingFactory.class);

	private UserTrackingFactory() {

	}

	public static IUserTracking getUserTrackingConnector() {
		if (!Utility.isUserTrackingEnabled()) {
			return null;
		}

		String method = Utility.getUserTrackingMethod();
		if (method == null) {
			classLogger.warn("User Tracking is enabled but could not find key for method ('"
					+ Constants.USER_TRACKING_METHOD + "')");
			return null;
		}

		if (method.equalsIgnoreCase(IUserTracking.GEO_IP2)) {
			return Geoip2UserTrackingUtils.getInstance();
		} else {
			classLogger.warn("User Tracking is enabled but could not find type for input = '" + method + "'");
			return null;
		}
	}
}
