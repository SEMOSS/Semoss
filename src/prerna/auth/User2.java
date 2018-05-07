package prerna.auth;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import prerna.om.AbstractValueObject;

public class User2 extends AbstractValueObject{
	
	// name of this user in the SEMOSS system if there is one
	
	// need to have an access token store
	Hashtable<String, AccessToken> accessTokens = new Hashtable<String, AccessToken>();
	List<String> loggedInProfiles = new ArrayList<String>();
	
	/**
	 * Set the access token for a given provider
	 * @param value
	 */
	public void setAccessToken(AccessToken value) {
		String name = value.getProvider();
		if(!loggedInProfiles.contains(name)) {
			loggedInProfiles.add(name);
		}
		accessTokens.put(name, value);
	}
	
	/**
	 * Get the requested access token
	 * @param name
	 * @return
	 */
	public AccessToken getAccessToken(String name) {
		return accessTokens.get(name);
	}
	
	/**
	 * Drop the access token for a given provider
	 * @param name			The name of the provider
	 * @return				boolean if the provider was dropped
	 */
	public boolean dropAccessToken(String name) {
		// remove from token map
		AccessToken token = accessTokens.remove(name);
		// remove from profiles list
		loggedInProfiles.remove(name);
		// return false if the token actually wasn't found
		if(token == null) {
			return false;
		}
		return true;
	}
	
	/**
	 * Get the list of logged in profiles
	 * @return
	 */
	public List<String> getLogins() {
		return loggedInProfiles;
	}

}
