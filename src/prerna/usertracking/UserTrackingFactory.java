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
package prerna.usertracking;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.usertracking.geoip2.Geoip2UserTrackingUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public class UserTrackingFactory {

  private static final Logger logger = LogManager.getLogger(UserTrackingFactory.class);

  private UserTrackingFactory() {}

  public static IUserTracking getUserTrackingConnector() {
    if (!Utility.isUserTrackingEnabled()) {
      return null;
    }

    String method = Utility.getUserTrackingMethod();
    if (method == null) {
      logger.warn(
          "User Tracking is enabled but could not find key for method ('"
              + Constants.USER_TRACKING_METHOD
              + "')");
      return null;
    }

    if (method.equalsIgnoreCase(IUserTracking.GEO_IP2)) {
      return Geoip2UserTrackingUtils.getInstance();
    } else {
      logger.warn("User Tracking is enabled but could not find type for input = '" + method + "'");
      return null;
    }
  }
}
