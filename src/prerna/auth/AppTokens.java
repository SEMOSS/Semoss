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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Hashtable;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.AbstractValueObject;
import prerna.security.HttpHelperUtility;
import prerna.util.SocialPropertiesUtil;
import prerna.util.git.GitRepoUtils;

public class AppTokens extends AbstractValueObject {

	protected static final Logger classLogger = LogManager.getLogger(AppTokens.class);

	// name of this user in the SEMOSS system if there is one

	private static AppTokens app = null;

	private static SocialPropertiesUtil socialData = null;
	private static AccessToken twitToken = null;
	private static AccessToken googAppToken = null;

	// need to have an access token store
	private Hashtable<AuthProvider, AccessToken> accessTokens = new Hashtable<AuthProvider, AccessToken>();

	private AppTokens() {

	}

	public static AppTokens getInstance() {
		if (app == null) {
			app = new AppTokens();
			loginGoogleApp();
			loginTwitterApp();

			if (googAppToken != null) {
				app.setAccessToken(googAppToken);
			}
			if (twitToken != null) {
				app.setAccessToken(twitToken);
			}

			socialData = SocialPropertiesUtil.getInstance();
		}
		return app;
	}

	public void setAccessToken(AccessToken value) {
		AuthProvider name = value.getProvider();
		accessTokens.put(name, value);
	}

	public AccessToken getAccessToken(AuthProvider name) {
		AccessToken token = accessTokens.get(name);
		if (token == null) {
			if (name == AuthProvider.TWITTER) {
				loginTwitterApp();
				if (twitToken != null) {
					app.setAccessToken(twitToken);
				}
			} else if (name == AuthProvider.GOOGLE_MAP) {
				loginGoogleApp();
				if (googAppToken != null) {
					app.setAccessToken(googAppToken);
				}
			}
			// try again...
			token = accessTokens.get(name);
		}
		return token;
	}

	public void dropAccessToken(AuthProvider name) {
		accessTokens.remove(name);
	}

	private static void loginGoogleApp() {
		// nothing big here
		// set the name on accesstoken
		if (socialData != null && googAppToken == null) {
			googAppToken = new AccessToken();
			googAppToken.setAccess_token(socialData.getProperty("google_maps_api"));
			googAppToken.setProvider(AuthProvider.GOOGLE_MAP);
		}
	}

	private static void loginTwitterApp() {
		if (twitToken != null) {
			return; // Token already exists
		}

		CloseableHttpClient httpclient = null;
		CloseableHttpResponse authResp = null;

		try {
			// Load credentials
			TwitterCredentials credentials = loadTwitterCredentials();
			if (credentials == null) {
				classLogger.warn("Twitter credentials not configured");
				return;
			}

			// Add SSL certificate support
			GitRepoUtils.addCertForDomain("https://twitter.com");

			// Create and encode credentials
			String jointString = credentials.getEncodedCredentials();

			// Build and execute request
			httpclient = HttpClients.createDefault();
			HttpPost httppost = new HttpPost("https://api.twitter.com/oauth2/token");
			httppost.addHeader("Authorization", "Basic " + jointString);
			httppost.addHeader("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");

			List<NameValuePair> paramList = new ArrayList<>();
			paramList.add(new BasicNameValuePair("grant_type", "client_credentials"));
			httppost.setEntity(new UrlEncodedFormEntity(paramList, "UTF-8"));

			authResp = httpclient.execute(httppost);

			int statusCode = authResp.getStatusLine().getStatusCode();
			classLogger.info("Twitter authentication response code: {}", statusCode);

			if (statusCode == 200) {
				String responseBody = readResponseBody(authResp);
				twitToken = HttpHelperUtility.getJAccessToken(responseBody);
				twitToken.setProvider(AuthProvider.TWITTER);
				classLogger.info("Successfully authenticated with Twitter API");
			} else {
				String errorBody = readResponseBody(authResp);
				classLogger.error("Twitter authentication failed with status {}: {}", statusCode, errorBody);
			}

			// Clear sensitive data
			credentials.clear();

		} catch (IOException ex) {
			classLogger.error("IOException during Twitter authentication", ex);
		} catch (Exception ex) {
			classLogger.error("Unexpected error during Twitter authentication", ex);
		} finally {
			closeQuietly(authResp);
			closeQuietly(httpclient);
		}
	}

	/**
	 * Helper class to securely manage Twitter credentials
	 */
	private static class TwitterCredentials {
		private char[] clientId;
		private char[] clientSecret;

		public TwitterCredentials(char[] clientId, char[] clientSecret) {
			this.clientId = clientId;
			this.clientSecret = clientSecret;
		}

		public String getEncodedCredentials() {
			String combined = new String(clientId) + ":" + new String(clientSecret);
			String encoded = Base64.getEncoder().encodeToString(combined.getBytes());
			overwriteString(combined);
			return encoded;
		}

		public void clear() {
			overwriteCharArray(clientId);
			overwriteCharArray(clientSecret);
		}

		private void overwriteString(String str) {
			str = null;
		}
	}

	/**
	 * Load Twitter credentials from social properties
	 */
	private static TwitterCredentials loadTwitterCredentials() {
		if (socialData == null) {
			return null;
		}

		String prefix = "twitter_";
		char[] clientId = null;
		char[] clientSecret = null;

		if (socialData.containsKey(prefix + "client_id")) {
			clientId = socialData.getProperty(prefix + "client_id").toCharArray();
		}
		if (socialData.containsKey(prefix + "secret_key")) {
			clientSecret = socialData.getProperty(prefix + "secret_key").toCharArray();
		}

		if (clientId == null || clientSecret == null) {
			if (clientId != null) {
				overwriteCharArray(clientId);
			}
			if (clientSecret != null) {
				overwriteCharArray(clientSecret);
			}
			return null;
		}

		return new TwitterCredentials(clientId, clientSecret);
	}

	/**
	 * Read response body from HTTP response
	 */
	private static String readResponseBody(CloseableHttpResponse response) throws IOException {
		if (response == null || response.getEntity() == null) {
			return "";
		}

		try (InputStream is = response.getEntity().getContent();
				InputStreamReader isr = new InputStreamReader(is, "UTF-8");
				BufferedReader rd = new BufferedReader(isr)) {

			StringBuilder result = new StringBuilder();
			String line;
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			return result.toString();
		}
	}

	private static void closeQuietly(AutoCloseable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
				classLogger.debug("Error closing resource: {}", e.getMessage());
			}
		}
	}

	private static void overwriteCharArray(char[] val) {
		if (val != null) {
			java.util.Arrays.fill(val, '\0');
		}
	}

}
