package prerna.usertracking;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.usertracking.geoip2.Geoip2UserTrackingUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public class UserTrackingFactory {

	private static final Logger logger = LogManager.getLogger(UserTrackingFactory.class);
	
	private UserTrackingFactory() {
		
	}
	
	public static IUserTracking getUserTrackingConnector() {
		if (!Utility.isUserTrackingEnabled()) {
			return null;
		}

		String method = Utility.getUserTrackingMethod();
		if(method == null) {
			logger.warn("User Tracking is enabled but could not find key for method ('" + Constants.USER_TRACKING_METHOD + "')");
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
