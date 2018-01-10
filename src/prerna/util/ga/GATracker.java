package prerna.util.ga;

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
		String trackingOn = DIHelper.getInstance().getProperty("GA_TRACKING");
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
