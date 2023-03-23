package prerna.util.linotp;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.AccessToken;

public class LinOTPResponse {

	private Map<String, Object> returnMap = null;
	private AccessToken token = null;
	private int responseCode = -1;
	
	public Map<String, Object> getReturnMap() {
		if(returnMap == null) {
			returnMap = new HashMap<>();
		}
		return returnMap;
	}
	
	public void setReturnMap(Map<String, Object> returnMap) {
		this.returnMap = returnMap;
	}
	
	public AccessToken getToken() {
		return token;
	}
	
	public void setToken(AccessToken token) {
		this.token = token;
	}
	
	public int getResponseCode() {
		return responseCode;
	}
	
	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}
	
}
