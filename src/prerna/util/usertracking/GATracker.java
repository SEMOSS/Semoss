package prerna.util.usertracking;

import prerna.util.DIHelper;

public class GATracker {

	private static IGoogleAnalytics instance;
	
	private GATracker() {
		
	}
	
	public static IGoogleAnalytics getInstance() {
		if(instance == null) {
			instance = createInstance();
		}
		return instance;
	}
	
	/**
	 * Determine if we should track based on key inside RDF_MAP
	 * @return
	 */
	private static IGoogleAnalytics createInstance() {
		String trackingOn = "true";
		try {
			trackingOn = DIHelper.getInstance().getProperty("GA_TRACKING");
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
			return new GoogleAnalytics();
		}
		return new NullGoogleAnalytics();
	}
}
