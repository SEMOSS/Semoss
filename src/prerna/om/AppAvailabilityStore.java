package prerna.om;

import java.util.HashSet;
import java.util.Set;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;

public class AppAvailabilityStore {

	private static AppAvailabilityStore store = null;
	
	// keep track of apps that have been disabled
	private Set<String> disabledApps = new HashSet<>();
	
	private AppAvailabilityStore() {
		
	}
	
	public static AppAvailabilityStore getInstance() {
		if(!AbstractSecurityUtils.securityEnabled()) {
			return null;
		}
		
		if(store != null) {
			return store;
		}

		synchronized(AppAvailabilityStore.class) {
			if(store == null) {
				store = new AppAvailabilityStore();
			}

			return store;
		}
	}
	
	/**
	 * Disable the app if the user is the owner
	 * @param user
	 * @param appId
	 * @return
	 */
	public boolean disableApp(User user, String appId) {
		// only the owner can disable the app
		if(!SecurityAppUtils.userIsOwner(user, appId)) {
			return false;
		}

		// add the disabled app to the list
		this.disabledApps.add(appId);
		return true;
	}
	
	/**
	 * Return a boolean if the app is disabled by the owner
	 * @param appId
	 * @return
	 */
	public boolean isAppDisabledByOwner(String appId) {
		return this.disabledApps.contains(appId);
	}
	
	public boolean enableApp(User user, String appId) {
		// only the owner can disable the app
		if(!SecurityAppUtils.userIsOwner(user, appId)) {
			return false;
		}

		// remove the appId from the list
		this.disabledApps.remove(appId);
		return true;
	}
	
}
