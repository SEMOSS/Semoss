package prerna.auth;

import java.io.Serializable;

import prerna.util.Utility;

public class InsightToken implements Serializable {

	// the secret
	private String secret = null;
	private boolean onetime = true;
	private String salt = null;
	
	public void setSecret(String secret) {
		this.secret = secret;
		genSalt();
	}
	
	public String getSecret() {
		return this.secret;
	}
	
	public boolean isOnetime() {
		return onetime;
	}
	
	public void setOnetime(boolean onetime) {
		this.onetime = onetime;
	}
	
	public void genSalt() {
		salt = Utility.getRandomString(10);
	}
	
	public String getSalt() {
		return salt;
	}
}
