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
package prerna.auth.mcp;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Store for OAuth Dynamic Client Registration (DCR).
 * Stores client metadata registered via the /oauth/register endpoint.
 *
 * This is an in-memory store for OAuth clients that register dynamically.
 * Client registrations do not persist across server restarts.
 */
public class MCPClientStore {

	private static final Logger classLogger = LogManager.getLogger(MCPClientStore.class);

	private static MCPClientStore instance;
	private Map<String, ClientRegistration> clients = new ConcurrentHashMap<>();

	/**
	 * Private constructor
	 */
	private MCPClientStore() {
	}

	public static synchronized MCPClientStore getInstance() {
		if (instance == null) {
			instance = new MCPClientStore();
		}
		return instance;
	}

	/**
	 * Register a new OAuth client
	 *
	 * @param clientId The client ID
	 * @param registration The client registration metadata
	 */
	public void registerClient(String clientId, ClientRegistration registration) {
		clients.put(clientId, registration);
		classLogger.info("Registered OAuth client: " + clientId);
	}

	/**
	 * Get a client registration by client ID
	 *
	 * @param clientId The client ID
	 * @return The client registration, or null if not found
	 */
	public ClientRegistration getClient(String clientId) {
		return clients.get(clientId);
	}

	/**
	 * Check if a redirect URI is valid for a given client
	 *
	 * @param clientId The client ID
	 * @param redirectUri The redirect URI to validate
	 * @return true if the redirect URI is registered for this client
	 */
	public boolean isValidRedirectUri(String clientId, String redirectUri) {
		ClientRegistration client = clients.get(clientId);
		if (client == null) {
			classLogger.warn("Client not found: " + clientId);
			return false;
		}
		return client.getRedirectUris().contains(redirectUri);
	}

	/**
	 * Inner class representing an OAuth client registration
	 */
	public static class ClientRegistration {
		private String clientId;
		private String clientSecret;
		private List<String> redirectUris;
		private List<String> grantTypes;
		private String tokenEndpointAuthMethod;
		private long registeredAt;

		public ClientRegistration(String clientId, String clientSecret, List<String> redirectUris,
								List<String> grantTypes, String tokenEndpointAuthMethod) {
			this.clientId = clientId;
			this.clientSecret = clientSecret;
			this.redirectUris = redirectUris;
			this.grantTypes = grantTypes;
			this.tokenEndpointAuthMethod = tokenEndpointAuthMethod;
			this.registeredAt = System.currentTimeMillis();
		}

		public String getClientId() {
			return clientId;
		}

		public String getClientSecret() {
			return clientSecret;
		}

		public List<String> getRedirectUris() {
			return redirectUris;
		}

		public List<String> getGrantTypes() {
			return grantTypes;
		}

		public String getTokenEndpointAuthMethod() {
			return tokenEndpointAuthMethod;
		}

		public long getRegisteredAt() {
			return registeredAt;
		}
	}
}
