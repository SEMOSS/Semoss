/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.util.usertracking;

import prerna.util.Constants;
import prerna.util.Utility;

public class UserTrackerFactory {

	private static IUserTracker instance;
	private static boolean tOn = false;

	private UserTrackerFactory() {
	}

	public static IUserTracker getInstance() {
		if (instance == null) {
			instance = createInstance();
		}
		return instance;
	}

	public static boolean isTracking() {
		return tOn;
	}

	/**
	 * Determine if we should track based on key inside RDF_MAP
	 *
	 * @return
	 */
	private static IUserTracker createInstance() {
		String trackingOn = "true";
		try {
			trackingOn = Utility.getDIHelperProperty(Constants.T_ON);
			// for the old key that was google analytics specific
			if (trackingOn == null) {
				trackingOn = Utility.getDIHelperProperty("GA_TRACKING");
			}
		} catch (Exception e) {
			// this happens if DIHelper isn't loaded
			// occurs when testing
			trackingOn = "false";
		}
		boolean track = true;
		if (trackingOn != null) {
			track = Boolean.valueOf(trackingOn);
		}
		if (track) {
			String endpoint = null;
			try {
				endpoint = Utility.getDIHelperProperty("T_ENDPOINT");
				if (endpoint == null) {
					// well, can't do much without an endpoint
					return new NullUserTracker();
				}
				// set the endpoint
				TrackRequestThread.setEndpoint(endpoint);
			} catch (Exception e) {
				// this happens if DIHelper isn't loaded
				// occurs when testing
			}
			tOn = true;
			return new TableUserTracker();
		}
		return new NullUserTracker();
	}
}
