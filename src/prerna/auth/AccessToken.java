package prerna.auth;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import prerna.date.SemossDate;

public class AccessToken implements Serializable {

	AuthProvider provider = null;
	
	// this will store all the groups that the user has
	// will be provided to us when the user logs in 
	// from an IDP
	Collection<String> userGroups = null;
	String userGroupType = null;
	
	String id = null;
	String username = null;
	String access_token = null;
	int expires_in = 0; // this is in seconds
	String token_type = "Bearer";
	long startTime = -1;
	
	String email = null;
	String name = null;
	String profile = null;
	String gender = null;
	String locale = null;
	String phone = null;
	String phoneExtension = null;
	String countryCode = null;
	int maxTokens = 0;
	double maxResponseTime = 0.0;
	String frequency = null;
	
	
	Map<String, String> sans = null;
	

	boolean locked = false;
	SemossDate lastLogin = null;
	SemossDate lastPasswordReset = null;
	
	
	public AccessToken() {
		this.userGroups = new HashSet<>();
		this.sans = new HashMap<>();
	}
	
	public void init() {
		startTime = System.currentTimeMillis();
	}
	
	public void setAccess_token(String accessToken) {
		this.access_token = accessToken;
	}

	public String getAccess_token() {
		return this.access_token;
	}

	public AuthProvider getProvider() {
		return provider;
	}

	public void setProvider(AuthProvider provider) {
		this.provider = provider;
	}
	
	public Collection<String> getUserGroups() {
		return userGroups;
	}

	public void setUserGroups(Set<String> userGroups) {
		this.userGroups = userGroups;
	}

	public String getUserGroupType() {
		return userGroupType;
	}

	public void setUserGroupType(String userGroupType) {
		this.userGroupType = userGroupType;
	}

	public void setExpires_in(int expires_in) {
		this.expires_in = expires_in;
	}
	
	public void setToken_type(String token_type) {
		this.token_type = token_type;
	}
	
	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public int getExpires_in() {
		return expires_in;
	}

	public String getToken_type() {
		return token_type;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getName() {
		if(this.name == null) {
			return this.username;
		}
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getProfile() {
		return profile;
	}

	public void setProfile(String profile) {
		this.profile = profile;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getLocale() {
		return locale;
	}

	public void setLocale(String local) {
		this.locale = local;
	}

	public String getId() {
		if(id == null) {
			return email;
		}
		return id;
	}

	public void setId(String id) {
		this.id = id.trim();
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}
	
	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getPhoneExtension() {
		return phoneExtension;
	}

	public void setPhoneExtension(String phoneExtension) {
		this.phoneExtension = phoneExtension;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public Map<String, String> getSAN() {
		return this.sans;
	}

	public void setSAN(String sanName, String sanValue) {
		this.sans.put(sanName, sanValue);
	}

	public boolean isLocked() {
		return locked;
	}

	public void setLocked(Boolean locked) {
		this.locked = locked;
	}

	public SemossDate getLastLogin() {
		return lastLogin;
	}

	public void setLastLogin(SemossDate lastLogin) {
		this.lastLogin = lastLogin;
	}

	public SemossDate getLastPasswordReset() {
		return lastPasswordReset;
	}

	public void setLastPasswordReset(SemossDate lastPasswordReset) {
		this.lastPasswordReset = lastPasswordReset;
	}

	public int getMaxToken() {
		return maxTokens;
	}

	public void setMaxTokens(int maxTokens) {
		this.maxTokens = maxTokens;
	}

	public double getMaxResponseTime() {
		return maxResponseTime;
	}

	public void setMaxResponseTime(double maxResponseTime) {
		this.maxResponseTime = maxResponseTime;
	}

	public String getFrequency() {
		return frequency;
	}

	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}
	
}
