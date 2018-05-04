package prerna.auth;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import prerna.om.AbstractValueObject;

public class User2 extends AbstractValueObject{
	
	// name of this user in the SEMOSS system if there is one
	
	
	// need to have an access token store
	Hashtable <String, AccessToken> accessTokens = new Hashtable<String, AccessToken>();
	List <String> loggedInProfiles = new ArrayList<String>();
	
	
	public void setAccessToken(AccessToken value)
	{
		String name = value.getProvider();
		if(!loggedInProfiles.contains(name))
			loggedInProfiles.add(name);
		accessTokens.put(name, value);
	}
	

	public AccessToken getAccessToken(String name)
	{
		return accessTokens.get(name);
	}
	
	public void dropAccessToken(String name)
	{
		accessTokens.remove(name);
	}
	
	public List getLogins()
	{
		return loggedInProfiles;
	}
	
	

}
