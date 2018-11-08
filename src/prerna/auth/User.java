package prerna.auth;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.om.AbstractValueObject;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;

public class User extends AbstractValueObject {
	
	// name of this user in the SEMOSS system if there is one
	
	// need to have an access token store
	private transient AbstractRJavaTranslator rJavaTranslator;
	private RConnection rConn; 
	
	Hashtable<AuthProvider, AccessToken> accessTokens = new Hashtable<AuthProvider, AccessToken>();
	List<AuthProvider> loggedInProfiles = new ArrayList<AuthProvider>();
	
	/**
	 * Set the access token for a given provider
	 * @param value
	 */
	public void setAccessToken(AccessToken value) {
		AuthProvider name = value.getProvider();
		if(!loggedInProfiles.contains(name)) {
			loggedInProfiles.add(name);
		}
		accessTokens.put(name, value);
	}
	
	/**
	 * Set the access token for a given provider
	 * We do not register in the logged in profiles but can still grab
	 * from reactors that utilize them
	 * @param value
	 */
	public void setGlobalAccessToken(AccessToken value) {
		AuthProvider name = value.getProvider();
		accessTokens.put(name, value);
	}
	
	/**
	 * Get the requested access token
	 * @param name
	 * @return
	 */
	public AccessToken getAccessToken(AuthProvider name) {
		return accessTokens.get(name);
	}
	
	/**
	 * Drop the access token for a given provider
	 * @param name			The name of the provider
	 * @return				boolean if the provider was dropped
	 */
	public boolean dropAccessToken(String name) {
		// remove from token map
		AuthProvider tokenKey = AuthProvider.valueOf(name);
		AccessToken token = accessTokens.remove(tokenKey);
		// remove from profiles list
		loggedInProfiles.remove(tokenKey);
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
	public List<AuthProvider> getLogins() {
		return loggedInProfiles;
	}

	public boolean isLoggedIn() {
		return !this.loggedInProfiles.isEmpty();
	}

	public AbstractRJavaTranslator getrJavaTranslator() {
		return rJavaTranslator;
	}

	public void setrJavaTranslator(AbstractRJavaTranslator rJavaTranslator) {
		this.rJavaTranslator = rJavaTranslator;
	}
	public RConnection getRConn() {
		return rConn;
	}

	public void setRConn(RConnection rCon) {
		this.rConn = rCon;
	}	
}
