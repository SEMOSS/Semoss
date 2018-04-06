package prerna.auth;

import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import prerna.om.AbstractValueObject;

public class User2 extends AbstractValueObject{
	
	// name of this user in the SEMOSS system if there is one
	
	
	// need to have an access token store
	Hashtable <String, AccessToken> accessTokens = new Hashtable<String, AccessToken>();
	
	
	public void setAccessToken(AccessToken value)
	{
		String name = value.getProvider();
		accessTokens.put(name, value);
	}
	

	public AccessToken getAccessToken(String name)
	{
		return accessTokens.get(name);
	}
	
	
	
	

}
