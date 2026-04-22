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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import prerna.io.connector.GenericTokenFiller;
import prerna.io.connector.github.GithubTokenFiller;
import prerna.io.connector.gitlab.GitLabTokenFiller;
import prerna.io.connector.google.GoogleTokenFiller;
import prerna.io.connector.jira.JiraTokenFiller;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.io.connector.okta.OktaTokenFiller;
import prerna.io.connector.salesforce.SalesforceTokenFiller;
import prerna.io.connector.servicenow.ServiceNowTokenFiller;

public enum AuthProvider implements Serializable {

	// @formatter:off
	FORGEROCK("FORGEROCK", "Forgerock", true, GenericTokenFiller.class.getName()),
	GOOGLE("GOOGLE", "Google", true, GoogleTokenFiller.class.getName()), 
	GOOGLE_MAP("GOOGLE_MAP", "GoogleMap", true, null),
	GITHUB("GITHUB", "GitHub", true, GithubTokenFiller.class.getName()),
	GITLAB("GITLAB", "GitLab", true, GitLabTokenFiller.class.getName()),
	JIRA("JIRA", "Jira", true, JiraTokenFiller.class.getName()),
	KEYCLOAK("KEYCLOAK", "Keycloak", true, GenericTokenFiller.class.getName()),
	MICROSOFT("MICROSOFT", "Microsoft", true, MicrosoftTokenFiller.class.getName()),
	SALESFORCE("SALESFORCE", "Salesforce", true, SalesforceTokenFiller.class.getName()),
	SERVICENOW("SERVICENOW", "ServiceNow", true, ServiceNowTokenFiller.class.getName()),
	SITEMINDER("SITEMINDER", "SiteMinder", true, null),
	SURVEYMONKEY("SURVEYMONKEY", "SurveyMonkey", true, null),
	ADFS("ADFS", "ADFS", true, null),
	OKTA("OKTA", "Okta", true, OktaTokenFiller.class.getName()),

	// native login
	NATIVE("NATIVE", "Native", false, null),
	// saml
	SAML("SAML", "SAML", false, null),
	// using ldap
	LDAP("LDAP", "Active Directory", false, null),
	// linOTP
	LINOTP("LINOTP", "LinOTP", false, null),
	
	// this one is kinda special ...
	CAC("CAC", "CAC", false, null),
	WINDOWS_USER("WINDOWS_USER", "Windows NLTM", false, null),
	API_USER("API_USER", "API Login", false, null),
	
	// these are not used as much ...
	TWITTER("TWITTER", "Twitter", true, null),
	DROPBOX("DROPBOX", "Dropbox", true, null),
	PRODUCT_HUNT("PRODUCT_HUNT", "Product Hunt", true, null),
	LINKEDIN("LINKEDIN", "LinkedIn", true, null),

	// catch all for other OAuth
	GENERIC("GENERIC", "Generic", true, GenericTokenFiller.class.getName()),
	;
	// @formatter:on

	private String label;
	private String displayName;
	private boolean isOAuth;
	private String tokenFillerClass;

	AuthProvider(String label, String displayName, boolean isOAuth, String tokenFillerClass) {
		this.label = label;
		this.displayName = displayName;
		this.isOAuth = isOAuth;
		this.tokenFillerClass = tokenFillerClass;
	}

	public String getLabel() {
		return label;
	}

	public String getDisplayName() {
		return displayName;
	}

	public boolean isOAuth() {
		return isOAuth;
	}

	public String getTokenFillerClass() {
		return tokenFillerClass;
	}

	@Override
	public String toString() {
		return getLabel();
	}

	public static AuthProvider getProviderFromString(String authProv) {
		AuthProvider provider = null;
		try {
			provider = AuthProvider.valueOf(authProv.toUpperCase());
		} catch (Exception e) {
			provider = AuthProvider.GENERIC;
		}
		return provider;
	}

	/**
	 * Get the keys are they should be in the social.properties files All keys
	 * should be the same as the enum name but lower case
	 * 
	 * @return
	 */
	public static Set<String> getSocialPropKeys() {
		Set<String> vals = new HashSet<>();
		for (AuthProvider auth : AuthProvider.values()) {
			vals.add(auth.name().toLowerCase());
		}
		// TODO: account for legacy MS
		vals.add("ms");
		return vals;
	}

	public static Map<String, AuthProvider> getSocialPropKeysToEnum() {
		Map<String, AuthProvider> vals = new HashMap<>();
		for (AuthProvider auth : AuthProvider.values()) {
			vals.put(auth.name().toLowerCase(), auth);
		}
		// TODO: account for legacy MS
		vals.put("ms", AuthProvider.MICROSOFT);

		return vals;
	}

	@Deprecated
	public static Map<String, String> getLabelToLegacyName() {
		Map<String, String> vals = new HashMap<>();
		for (AuthProvider auth : AuthProvider.values()) {
			vals.put(auth.label, auth.getLegacyName());
		}
		// TODO: account for legacy MS
		vals.put(AuthProvider.MICROSOFT.label, "Ms");

		return vals;
	}

	/**
	 * Really gross looking... you get things like "Ms", "Cac"... IF CREATING NEW
	 * LOGIC, PLEASE USE AuthProvider.name / getLabel
	 */
	@Deprecated
	private String getLegacyName() {
		return name().charAt(0) + name().substring(1).toLowerCase();
	}

}
