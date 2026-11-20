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

		// copy all scalar fields straight across via the protected hook (raw values, no
		// setter validation or getter fallbacks). The public setters cannot be used
		// here
		// because this class overrides them to throw.
		newToken.copyScalarFieldsFrom(token);

		// install unmodifiable views of the collection-valued fields so they cannot be
		// mutated through this read-only token
		Collection<String> userGroups = token.getUserGroups();
		if (userGroups != null) {
			newToken.setUserGroupsInternal(Collections.unmodifiableCollection(userGroups));
		}
		Map<String, String> sans = token.getSAN();
		if (sans != null) {
			newToken.setSansInternal(Collections.unmodifiableMap(sans));
		}

		// setMeta on a ReadOnlyAccessToken already makes a deep, mutable copy of the
		// map,
		// so callers can still update metadata via the AccessToken helper methods
		// (e.g.,
		// addMetaValue) while every other field stays immutable.
		newToken.setMeta(token.getMeta());

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
		// The base setMeta is the raw assignment, so route the copy through super to
		// store
		// it (this class' own setMeta override would otherwise be the one invoked).
		if (meta == null) {
			super.setMeta(null);
			return;
		}
		Map<String, Collection<String>> deepMutableMeta = meta.entrySet().stream().collect(Collectors.toMap(
				Map.Entry::getKey, e -> (e.getValue() == null) ? null : new java.util.ArrayList<>(e.getValue())));
		super.setMeta(new java.util.HashMap<>(deepMutableMeta));
	}

	@Override
	public Map<String, Collection<String>> getMeta() {
		Map<String, Collection<String>> meta = super.getMeta();
		if (meta == null) {
			return null;
		}
		return Collections.unmodifiableMap(meta);
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