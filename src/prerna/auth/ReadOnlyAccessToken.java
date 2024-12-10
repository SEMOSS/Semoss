package prerna.auth;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import prerna.date.SemossDate;

public class ReadOnlyAccessToken extends AccessToken implements Serializable {

	public static AccessToken unmodifiableToken(AccessToken token) {
		ReadOnlyAccessToken newToken = new ReadOnlyAccessToken();
		newToken.provider = token.provider;
		if(token.userGroups != null) {
			newToken.userGroups = Collections.unmodifiableCollection(token.userGroups);
		}
		newToken.userGroupType = token.userGroupType;
		newToken.id = token.id;
		newToken.username = token.username;
		newToken.access_token = token.access_token;
		newToken.expires_in = token.expires_in;
		newToken.token_type = token.token_type;
		newToken.startTime = token.startTime;
		newToken.email = token.email;
		newToken.name = token.name;
		newToken.profile = token.profile;
		newToken.gender = token.gender;
		newToken.locale = token.locale;
		newToken.phone = token.phone;
		newToken.phoneExtension = token.phoneExtension;
		newToken.countryCode = token.countryCode;
		if(token.sans != null) {
			newToken.sans = Collections.unmodifiableMap(token.sans);
		}
		newToken.locked = token.locked;
		newToken.lastLogin = token.lastLogin;
		newToken.lastPasswordReset = token.lastPasswordReset;
		return newToken;
	}
	
	/*
	 * No set operations are allowed
	 */
	
	public void setAccess_token(String accessToken) {
		throw new IllegalArgumentException("This object cannot be modified");
	}
	
	public void setProvider(AuthProvider provider) {
		throw new IllegalArgumentException("This object cannot be modified");
	}
	
	public void setUserGroups(Set<String> userGroups) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setUserGroupType(String userGroupType) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setExpires_in(int expires_in) {
		throw new IllegalArgumentException("This object cannot be modified");
	}
	
	public void setToken_type(String token_type) {
		throw new IllegalArgumentException("This object cannot be modified");
	}
	
	public void setEmail(String email) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setName(String name) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setProfile(String profile) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setGender(String gender) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setLocale(String local) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setId(String id) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setUsername(String username) {
		throw new IllegalArgumentException("This object cannot be modified");
	}
	
	public void setPhone(String phone) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setPhoneExtension(String phoneExtension) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setCountryCode(String countryCode) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setSAN(String sanName, String sanValue) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setLocked(Boolean locked) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setLastLogin(SemossDate lastLogin) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	public void setLastPasswordReset(SemossDate lastPasswordReset) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

}
