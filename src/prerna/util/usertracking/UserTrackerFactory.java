package prerna.util.usertracking;

import prerna.util.DIHelper;

public class UserTrackerFactory {

	private static IUserTracker instance;
	
	private UserTrackerFactory() {
		
	}
	
	public static IUserTracker getInstance() {
		if(instance == null) {
			instance = createInstance();
		}
		return instance;
	}
	
	/**
	 * Determine if we should track based on key inside RDF_MAP
	 * @return
	 */
	private static IUserTracker createInstance() {
		String trackingOn = "true";
		try {
			trackingOn = DIHelper.getInstance().getProperty("T_ON");
			// for the old key that was google analytics specific
			if(trackingOn == null) {
				trackingOn = DIHelper.getInstance().getProperty("GA_TRACKING");
			}
		} catch(Exception e) {
			// this happens if DIHelper isn't loaded
			// occurs when testing
			trackingOn = "false";
		}
		boolean track = true;
		if(trackingOn != null) {
			track = Boolean.valueOf(trackingOn);
		}
		if(track) {
			String endpoint = null;
			try {
				endpoint = DIHelper.getInstance().getProperty("T_ENDPOINT");
				// set the endpoint
				TrackRequestThread.setEndpoint(endpoint);
			} catch(Exception e) {
				// this happens if DIHelper isn't loaded
				// occurs when testing
			}
			return new TableUserTracker();
		}
		return new NullUserTracker();
	}
}
