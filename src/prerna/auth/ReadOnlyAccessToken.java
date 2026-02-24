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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import prerna.date.SemossDate;

public class ReadOnlyAccessToken extends AccessToken implements Serializable {

	private static final long serialVersionUID = 1L;

	public static AccessToken unmodifiableToken(AccessToken token) {
		ReadOnlyAccessToken newToken = new ReadOnlyAccessToken();
		newToken.provider = token.provider;
		if (token.userGroups != null) {
			newToken.userGroups = Collections.unmodifiableCollection(token.userGroups);
		}
		newToken.userGroupType = token.userGroupType;
		newToken.id = token.id;
		newToken.username = token.username;
		newToken.access_token = token.access_token;
		newToken.instance_url = token.instance_url;
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
		if (token.sans != null) {
			newToken.sans = Collections.unmodifiableMap(token.sans);
		}
		if (token.meta != null) {
			// Make a deep mutable copy of meta so callers can update metadata via
			// AccessToken helper methods (e.g., addMetaValue). We still prevent
			// modification of other AccessToken fields by throwing from setters.
			Map<String, Collection<String>> deepMutableMeta = token.meta.entrySet().stream().collect(Collectors.toMap(
					Map.Entry::getKey, e -> (e.getValue() == null) ? null : new java.util.ArrayList<>(e.getValue())));
			newToken.meta = new java.util.HashMap<>(deepMutableMeta);
		}
		newToken.locked = token.locked;
		newToken.lastLogin = token.lastLogin;
		newToken.lastPasswordReset = token.lastPasswordReset;

		newToken.modelMaxTokens = token.modelMaxTokens;
		newToken.modelMaxResponseTime = token.modelMaxResponseTime;
		newToken.modelUsageFrequency = token.modelUsageFrequency;
		newToken.modelUsageRestriction = token.modelUsageRestriction;

		return newToken;
	}

	/*
	 * No set operations are allowed
	 */

	@Override
	public void setAccess_token(String accessToken) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

    @Override
	public void setInstance_url(String instanceUrl) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setProvider(AuthProvider provider) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setUserGroups(Set<String> userGroups) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setUserGroupType(String userGroupType) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setExpires_in(int expires_in) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setToken_type(String token_type) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setEmail(String email) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setName(String name) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setProfile(String profile) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setGender(String gender) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setLocale(String local) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setId(String id) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setUsername(String username) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setPhone(String phone) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setPhoneExtension(String phoneExtension) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setCountryCode(String countryCode) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setSAN(String sanName, String sanValue) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setMeta(Map<String, Collection<String>> meta) {
		// Allow updating metadata on a read-only token by making a deep mutable copy.
		if (meta == null) {
			this.meta = null;
			return;
		}
		Map<String, Collection<String>> deepMutableMeta = meta.entrySet().stream().collect(Collectors.toMap(
				Map.Entry::getKey, e -> (e.getValue() == null) ? null : new java.util.ArrayList<>(e.getValue())));
		this.meta = new java.util.HashMap<>(deepMutableMeta);
	}

	@Override
	public Map<String, Collection<String>> getMeta() {
		if (this.meta == null) {
			return null;
		}
		return Collections.unmodifiableMap(this.meta);
	}

	@Override
	public Collection<String> getMetaValues(String key) {
		Collection<String> vals = super.getMetaValues(key);
		if (vals == null) {
			return null;
		}
		return Collections.unmodifiableCollection(vals);
	}

	@Override
	public void setLocked(Boolean locked) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setLastLogin(SemossDate lastLogin) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setLastPasswordReset(SemossDate lastPasswordReset) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setModelMaxTokens(int modelMaxTokens) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setModelMaxResponseTime(double modelMaxResponseTime) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setModelUsageFrequency(String modelUsageFrequency) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

	@Override
	public void setModelUsageRestriction(String modelUsageRestriction) {
		throw new IllegalArgumentException("This object cannot be modified");
	}

}