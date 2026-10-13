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
package prerna.reactor.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;

/**
 * Encodes and reconstructs the credential-free principal representation used by
 * the supported scheduler execution endpoint.
 */
public final class SchedulerUserAccess {

	private SchedulerUserAccess() {
	}

	public static String encode(User user) {
		if (user == null || user.getLogins().isEmpty()) {
			throw new IllegalArgumentException("Automation execution requires an authenticated user.");
		}
		List<String> access = new ArrayList<>();
		for (AuthProvider provider : user.getLogins()) {
			AccessToken token = user.getAccessToken(provider);
			if (token == null || token.getId() == null || token.getId().isBlank()) {
				throw new IllegalArgumentException("Authenticated scheduler principal is missing a user identifier.");
			}
			if (token.getId().indexOf(',') >= 0) {
				throw new IllegalArgumentException("Authenticated scheduler principal has an unsupported identifier.");
			}
			access.add(provider.name() + ":" + token.getId());
		}
		return String.join(",", access);
	}

	public static User reconstruct(String principalAccess) {
		if (principalAccess == null || principalAccess.isBlank()) {
			throw new IllegalArgumentException("Queued Automation run is missing its scheduler principal.");
		}
		User user = new User();
		AuthProvider primary = null;
		for (String accessPair : principalAccess.split(",")) {
			String[] providerAndId = accessPair.split(":", 2);
			if (providerAndId.length != 2 || providerAndId[1].isBlank()) {
				throw new IllegalArgumentException("Queued Automation run has an invalid scheduler principal.");
			}
			AuthProvider provider;
			try {
				provider = AuthProvider.valueOf(providerAndId[0]);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("Queued Automation run has an unsupported auth provider.", e);
			}
			AccessToken token = new AccessToken();
			token.setProvider(provider);
			token.setId(providerAndId[1]);
			token.setName("scheduler_" + UUID.randomUUID());
			user.setAccessToken(token);
			if (primary == null) {
				primary = provider;
			}
		}
		user.setPrimaryLogin(primary);
		return user;
	}
}
