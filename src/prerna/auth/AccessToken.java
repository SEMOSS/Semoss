/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.auth;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import prerna.date.SemossDate;

public class AccessToken implements Serializable {

	private static final long serialVersionUID = 1L;

	private AuthProvider provider = null;

	// this will store all the groups that the user has
	// will be provided to us when the user logs in
	// from an IDP
	private Collection<String> userGroups = null;
	private String userGroupType = null;

	private String id = null;
	private String username = null;
	private String access_token = null;
	private String instance_url = null;
	private int expires_in = 0; // this is in seconds
	private String token_type = "Bearer";
	private long startTime = -1;

	private String email = null;
	private String name = null;
	private String profile = null;
	private String gender = null;
	private String locale = null;
	private String phone = null;
	private String phoneExtension = null;
	private String countryCode = null;

	private int modelMaxTokens = 0;
	private double modelMaxResponseTime = 0.0;
	private String modelUsageFrequency = null;
	private String modelUsageRestriction = null;

	private Map<String, String> sans = null;

	private Map<String, Collection<String>> meta = null;

	private boolean locked = false;
	private SemossDate lastLogin = null;
	private SemossDate lastPasswordReset = null;

	/**
	 * Constructs a new AccessToken.
	 */
	public AccessToken() {
		this.userGroups = new HashSet<>();
		this.sans = new HashMap<>();
	}

	/**
	 * Initializes the token by setting its start time to the current system time.
	 * Used to track when the token was created for expiration calculations.
	 */
	public void init() {
		startTime = System.currentTimeMillis();
	}

	/**
	 * Sets the access token string.
	 * 
	 * @param accessToken the token value to set
	 */
	public void setAccess_token(String accessToken) {
		this.access_token = accessToken;
	}

	/**
	 * Gets the access token string.
	 * 
	 * @return the access token value
	 */
	public String getAccess_token() {
		return this.access_token;
	}

	public void setInstance_url(String instanceUrl) {
		this.instance_url = instanceUrl;
	}

	public String getInstance_url() {
		return this.instance_url;
	}

	/**
	 * Gets the authentication provider associated with this token.
	 * 
	 * @return the AuthProvider instance
	 */
	public AuthProvider getProvider() {
		return provider;
	}

	/**
	 * Sets the authentication provider for this token.
	 * 
	 * @param provider the AuthProvider to associate with this token
	 */
	public void setProvider(AuthProvider provider) {
		this.provider = provider;
	}

	/**
	 * Gets the collection of groups this user belongs to.
	 * 
	 * @return collection of group names, may be empty
	 */
	public Collection<String> getUserGroups() {
		return userGroups;
	}

	/**
	 * Sets the user's group membership.
	 * 
	 * @param userGroups set of group names to assign to this user
	 */
	public void setUserGroups(Set<String> userGroups) {
		this.userGroups = userGroups;
	}

	/**
	 * Gets the type/classification of user groups (e.g., LDAP, AD, custom).
	 * 
	 * @return the user group type identifier
	 */
	public String getUserGroupType() {
		return userGroupType;
	}

	/**
	 * Sets the type/classification of user groups.
	 * 
	 * @param userGroupType the group type identifier
	 */
	public void setUserGroupType(String userGroupType) {
		this.userGroupType = userGroupType;
	}

	/**
	 * Sets the token expiration time in seconds.
	 * 
	 * @param expires_in expiration duration in seconds
	 */
	public void setExpires_in(int expires_in) {
		this.expires_in = expires_in;
	}

	/**
	 * Sets the token type (typically "Bearer").
	 * 
	 * @param token_type the type of token
	 */
	public void setToken_type(String token_type) {
		this.token_type = token_type;
	}

	/**
	 * Gets the timestamp when this token was initialized.
	 * 
	 * @return millisecond timestamp of token creation
	 */
	public long getStartTime() {
		return startTime;
	}

	/**
	 * Sets the token initialization timestamp.
	 * 
	 * @param startTime millisecond timestamp
	 */
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	/**
	 * Gets the token expiration time in seconds.
	 * 
	 * @return expiration duration in seconds
	 */
	public int getExpires_in() {
		return expires_in;
	}

	/**
	 * Gets the token type.
	 * 
	 * @return the token type
	 */
	public String getToken_type() {
		return token_type;
	}

	/**
	 * Gets the user's email address.
	 * 
	 * @return the email address
	 */
	public String getEmail() {
		return email;
	}

	/**
	 * Sets the user's email address.
	 * 
	 * @param email the email address
	 */
	public void setEmail(String email) {
		this.email = email;
	}

	/**
	 * Gets the user's display name. Falls back to username if name is not set.
	 * 
	 * @return the user's display name or username if name is null
	 */
	public String getName() {
		if (this.name == null) {
			return this.username;
		}
		return name;
	}

	/**
	 * Sets the user's display name.
	 * 
	 * @param name the user's display name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Gets the user's profile information/URL.
	 * 
	 * @return the profile information
	 */
	public String getProfile() {
		return profile;
	}

	/**
	 * Sets the user's profile information/URL.
	 * 
	 * @param profile the profile information
	 */
	public void setProfile(String profile) {
		this.profile = profile;
	}

	/**
	 * Gets the user's gender.
	 * 
	 * @return the gender value
	 */
	public String getGender() {
		return gender;
	}

	/**
	 * Sets the user's gender.
	 * 
	 * @param gender the gender value
	 */
	public void setGender(String gender) {
		this.gender = gender;
	}

	/**
	 * Gets the user's preferred locale.
	 * 
	 * @return the locale identifier
	 */
	public String getLocale() {
		return locale;
	}

	/**
	 * Sets the user's preferred locale.
	 * 
	 * @param local the locale identifier
	 */
	public void setLocale(String local) {
		this.locale = local;
	}

	/**
	 * Gets the user's unique identifier. Falls back to email if ID is not set.
	 * 
	 * @return the user ID or email if ID is null
	 */
	public String getId() {
		if (id == null) {
			return email;
		}
		return id;
	}

	/**
	 * Sets the user's unique identifier (trimmed).
	 * 
	 * @param id the user ID
	 */
	public void setId(String id) {
		this.id = id.trim();
	}

	/**
	 * Gets the user's login username.
	 * 
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Sets the user's login username.
	 * 
	 * @param username the username
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Gets the user's phone number.
	 * 
	 * @return the phone number
	 */
	public String getPhone() {
		return phone;
	}

	/**
	 * Sets the user's phone number.
	 * 
	 * @param phone the phone number
	 */
	public void setPhone(String phone) {
		this.phone = phone;
	}

	/**
	 * Gets the extension for the user's phone number.
	 * 
	 * @return the phone extension
	 */
	public String getPhoneExtension() {
		return phoneExtension;
	}

	/**
	 * Sets the extension for the user's phone number.
	 * 
	 * @param phoneExtension the phone extension
	 */
	public void setPhoneExtension(String phoneExtension) {
		this.phoneExtension = phoneExtension;
	}

	/**
	 * Gets the user's country code.
	 * 
	 * @return the country code
	 */
	public String getCountryCode() {
		return countryCode;
	}

	/**
	 * Sets the user's country code.
	 * 
	 * @param countryCode the country code
	 */
	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	/**
	 * Gets the Subject Alternative Names (SAN) map for this token.
	 * 
	 * @return map of SAN names to values
	 */
	public Map<String, String> getSAN() {
		return this.sans;
	}

	/**
	 * Adds a Subject Alternative Name (SAN) value.
	 * 
	 * @param sanName  the SAN key
	 * @param sanValue the SAN value
	 */
	public void setSAN(String sanName, String sanValue) {
		this.sans.put(sanName, sanValue);
	}

	/**
	 * Returns the metadata map for this token.
	 * <p>
	 * Note: callers should treat the returned map as a snapshot of metadata. For
	 * ReadOnlyAccessToken instances the returned map may be unmodifiable; prefer
	 * {@link #getMetaValues(String)} when you need values for a specific key.
	 *
	 * @return map from meta key to collection of values, may be null when no
	 *         metadata is present
	 */
	public Map<String, Collection<String>> getMeta() {
		return this.meta;
	}

	/**
	 * Replace the entire metadata map for this token.
	 * <p>
	 * This method performs a shallow replace; callers who need to ensure a deep
	 * copy should copy the collections before calling. For ReadOnlyAccessToken
	 * instances this method will create a deep mutable copy of the provided map.
	 *
	 * @param meta new metadata map (may be null to clear metadata)
	 */
	public void setMeta(Map<String, Collection<String>> meta) {
		this.meta = meta;
	}

	/**
	 * Add a single metadata value to the collection for a key. Creates the meta map
	 * or the collection for the key if necessary. Duplicate values will be ignored.
	 *
	 * @param key   metadata key (must not be null)
	 * @param value value to add (may be null)
	 * @throws IllegalArgumentException if key is null
	 */
	public void addMetaValue(String key, String value) {
		if (key == null) {
			throw new IllegalArgumentException("meta key cannot be null");
		}
		if (this.meta == null) {
			this.meta = new HashMap<>();
		}
		Collection<String> vals = this.meta.get(key);
		if (vals == null) {
			vals = new ArrayList<>();
			this.meta.put(key, vals);
		}
		if (!vals.contains(value)) {
			vals.add(value);
		}
	}

	/**
	 * Add multiple metadata values for a key (appends to any existing values).
	 * Duplicate values are ignored.
	 *
	 * @param key    metadata key (must not be null)
	 * @param values collection of values to append (if null or empty this method is
	 *               a no-op)
	 * @throws IllegalArgumentException if key is null
	 */
	public void addMetaValues(String key, Collection<String> values) {
		if (key == null) {
			throw new IllegalArgumentException("meta key cannot be null");
		}
		if (values == null || values.isEmpty()) {
			return;
		}
		if (this.meta == null) {
			this.meta = new HashMap<>();
		}
		Collection<String> vals = this.meta.get(key);
		if (vals == null) {
			vals = new ArrayList<>();
			this.meta.put(key, vals);
		}
		for (String v : values) {
			if (!vals.contains(v)) {
				vals.add(v);
			}
		}
	}

	/**
	 * Remove a single metadata value for a key.
	 *
	 * @param key   metadata key (may be null)
	 * @param value value to remove
	 * @return true if a value was removed, false otherwise
	 */
	public boolean removeMetaValue(String key, String value) {
		if (this.meta == null || key == null) {
			return false;
		}
		Collection<String> vals = this.meta.get(key);
		if (vals == null) {
			return false;
		}
		boolean removed = vals.remove(value);
		if ((vals == null || vals.isEmpty()) && this.meta != null) {
			this.meta.remove(key);
		}
		return removed;
	}

	/**
	 * Remove an entire metadata key and its values.
	 *
	 * @param key metadata key to remove
	 * @return true if a mapping existed and was removed
	 */
	public boolean removeMetaKey(String key) {
		if (this.meta == null || key == null) {
			return false;
		}
		return this.meta.remove(key) != null;
	}

	/**
	 * Helper to get the collection of values for a meta key (may return null).
	 *
	 * @param key metadata key
	 * @return collection of strings for the key or null if none exists
	 */
	public Collection<String> getMetaValues(String key) {
		if (this.meta == null || key == null) {
			return null;
		}
		return this.meta.get(key);
	}

	/**
	 * Gets the locked status of this token.
	 * 
	 * @return true if the token is locked, false otherwise
	 */
	public boolean isLocked() {
		return locked;
	}

	/**
	 * Sets the locked status of this token.
	 * 
	 * @param locked true to lock the token, false to unlock
	 */
	public void setLocked(Boolean locked) {
		this.locked = locked;
	}

	/**
	 * Gets the timestamp of the user's last login.
	 * 
	 * @return the last login date/time
	 */
	public SemossDate getLastLogin() {
		return lastLogin;
	}

	/**
	 * Sets the timestamp of the user's last login.
	 * 
	 * @param lastLogin the last login date/time
	 */
	public void setLastLogin(SemossDate lastLogin) {
		this.lastLogin = lastLogin;
	}

	/**
	 * Gets the timestamp of the user's last password reset.
	 * 
	 * @return the last password reset date/time
	 */
	public SemossDate getLastPasswordReset() {
		return lastPasswordReset;
	}

	/**
	 * Sets the timestamp of the user's last password reset.
	 * 
	 * @param lastPasswordReset the last password reset date/time
	 */
	public void setLastPasswordReset(SemossDate lastPasswordReset) {
		this.lastPasswordReset = lastPasswordReset;
	}

	/**
	 * Gets the maximum number of tokens the user can use for model requests.
	 * 
	 * @return maximum token count
	 */
	public int getModelMaxTokens() {
		return modelMaxTokens;
	}

	/**
	 * Sets the maximum number of tokens for model requests.
	 * 
	 * @param modelMaxTokens maximum token count
	 */
	public void setModelMaxTokens(int modelMaxTokens) {
		this.modelMaxTokens = modelMaxTokens;
	}

	/**
	 * Gets the maximum response time allowed for model requests in seconds.
	 * 
	 * @return maximum response time in seconds
	 */
	public double getModelMaxResponseTime() {
		return modelMaxResponseTime;
	}

	/**
	 * Sets the maximum response time allowed for model requests.
	 * 
	 * @param modelMaxResponseTime maximum response time in seconds
	 */
	public void setModelMaxResponseTime(double modelMaxResponseTime) {
		this.modelMaxResponseTime = modelMaxResponseTime;
	}

	/**
	 * Gets the model usage frequency restriction (e.g., rate limiting).
	 * 
	 * @return the usage frequency restriction
	 */
	public String getModelUsageFrequency() {
		return modelUsageFrequency;
	}

	/**
	 * Sets the model usage frequency restriction.
	 * 
	 * @param modelUsageFrequency the usage frequency restriction
	 */
	public void setModelUsageFrequency(String modelUsageFrequency) {
		this.modelUsageFrequency = modelUsageFrequency;
	}

	/**
	 * Gets the model usage restriction policy.
	 * 
	 * @return the usage restriction policy
	 */
	public String getModelUsageRestriction() {
		return modelUsageRestriction;
	}

	/**
	 * Sets the model usage restriction policy.
	 * 
	 * @param modelUsageRestriction the usage restriction policy
	 */
	public void setModelUsageRestriction(String modelUsageRestriction) {
		this.modelUsageRestriction = modelUsageRestriction;
	}

	/**
	 * Copies all scalar (non-collection) field values from {@code source} into this
	 * instance.
	 * <p>
	 * This is a protected hook for subclasses (e.g. {@link ReadOnlyAccessToken})
	 * that need to populate their inherited fields without going through the public
	 * setters. Values are copied verbatim, bypassing getter fallbacks (such as
	 * {@link #getId()} returning the email) and setter validation (such as
	 * {@link #setId(String)} trimming). The collection/map fields (userGroups,
	 * sans, meta) are intentionally left untouched so the caller controls how those
	 * are shared or duplicated.
	 *
	 * @param source the token to copy scalar field values from
	 */
	protected void copyScalarFieldsFrom(AccessToken source) {
		this.provider = source.provider;
		this.userGroupType = source.userGroupType;
		this.id = source.id;
		this.username = source.username;
		this.access_token = source.access_token;
		this.instance_url = source.instance_url;
		this.expires_in = source.expires_in;
		this.token_type = source.token_type;
		this.startTime = source.startTime;
		this.email = source.email;
		this.name = source.name;
		this.profile = source.profile;
		this.gender = source.gender;
		this.locale = source.locale;
		this.phone = source.phone;
		this.phoneExtension = source.phoneExtension;
		this.countryCode = source.countryCode;
		this.modelMaxTokens = source.modelMaxTokens;
		this.modelMaxResponseTime = source.modelMaxResponseTime;
		this.modelUsageFrequency = source.modelUsageFrequency;
		this.modelUsageRestriction = source.modelUsageRestriction;
		this.locked = source.locked;
		this.lastLogin = source.lastLogin;
		this.lastPasswordReset = source.lastPasswordReset;
	}

	/**
	 * Directly replaces the backing user-groups collection.
	 * <p>
	 * Protected hook allowing subclasses to install an alternate view (for example
	 * an unmodifiable wrapper) without going through {@link #setUserGroups(Set)}.
	 *
	 * @param userGroups the collection to store as-is
	 */
	protected void setUserGroupsInternal(Collection<String> userGroups) {
		this.userGroups = userGroups;
	}

	/**
	 * Directly replaces the backing SAN map.
	 * <p>
	 * Protected hook allowing subclasses to install an alternate view (for example
	 * an unmodifiable wrapper) without going through
	 * {@link #setSAN(String, String)}.
	 *
	 * @param sans the map to store as-is
	 */
	protected void setSansInternal(Map<String, String> sans) {
		this.sans = sans;
	}

	/**
	 * Create a copy of the provided AccessToken.
	 * <p>
	 * This method constructs a new AccessToken instance and copies scalar fields
	 * directly. Collection and map fields are copied to new instances where
	 * possible (shallow copy of contained strings), so the returned token has
	 * independent collections and maps but shares immutable string instances.
	 *
	 * @param token the token to copy; may be null (method returns a new empty
	 *              token)
	 * @return a new AccessToken instance with copied fields; never null
	 */
	public static AccessToken copyToken(AccessToken token) {
		AccessToken newToken = new AccessToken();
		if (token == null) {
			return newToken;
		}

		newToken.provider = token.provider;
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
		newToken.instance_url = token.instance_url;

		// model-related fields
		newToken.modelMaxTokens = token.modelMaxTokens;
		newToken.modelMaxResponseTime = token.modelMaxResponseTime;
		newToken.modelUsageFrequency = token.modelUsageFrequency;
		newToken.modelUsageRestriction = token.modelUsageRestriction;

		if (token.userGroups != null) {
			newToken.userGroups = new ArrayList<>(token.userGroups);
		}

		if (token.sans != null) {
			newToken.sans = new HashMap<>(token.sans);
		}

		if (token.meta != null) {
			Map<String, Collection<String>> deepMeta = new HashMap<>();
			for (Map.Entry<String, Collection<String>> e : token.meta.entrySet()) {
				Collection<String> vals = e.getValue();
				deepMeta.put(e.getKey(), vals == null ? null : new ArrayList<>(vals));
			}
			newToken.meta = deepMeta;
		}

		newToken.locked = token.locked;
		newToken.lastLogin = token.lastLogin;
		newToken.lastPasswordReset = token.lastPasswordReset;

		return newToken;
	}
}