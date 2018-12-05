package prerna.auth;

import prerna.util.Utility;

public class InsightToken {

	// the secret
	String secret = null;
	boolean onetime = true;
	String salt = null;
	
	public void setSecret(String secret)
	{
		this.secret = secret;
		genSalt();
	}
	
	public String getSecret()
	{
		return this.secret;
	}
	
	public boolean isOnetime()
	{
		return onetime;
	}
	
	public void setOnetime()
	{
		this.onetime = onetime;
	}
	
	public void genSalt()
	{
		salt = Utility.getRandomString(10);
	}
	
	public String getSalt()
	{
		return salt;
	}
}
