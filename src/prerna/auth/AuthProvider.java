package prerna.auth;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import prerna.io.connector.GenericTokenFiller;
import prerna.io.connector.github.GithubTokenFiller;
import prerna.io.connector.gitlab.GitLabTokenFiller;
import prerna.io.connector.google.GoogleTokenFiller;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.io.connector.okta.OktaTokenFiller;

public enum AuthProvider implements Serializable {

	GOOGLE("GOOGLE", true, GoogleTokenFiller.class.getName()), 
	GOOGLE_MAP("GOOGLE_MAP", true, null),
	GITHUB("GITHUB", true, GithubTokenFiller.class.getName()),
	GITLAB("GITLAB", true, GitLabTokenFiller.class.getName()),
	//TODO: build out custom endpoint in UserResource
	KEYCLOAK("KEYCLOAK", true, GenericTokenFiller.class.getName()),
	MS("MS", true, MicrosoftTokenFiller.class.getName()), // this is azure graph
	SALESFORCE("SALESFORCE", true, null), 
	SITEMINDER("SITEMINDER", true, null),
	SURVEYMONKEY("SURVEYMONKEY", true, null),
	ADFS("ADFS", true, null),
	OKTA("OKTA", true, OktaTokenFiller.class.getName()),

	// native login
	NATIVE("NATIVE", false, null),
	// saml
	SAML("SAML", false, null),
	// using ldap
	ACTIVE_DIRECTORY("ACTIVE_DIRECTORY", false, null),
	// linOTP
	LINOTP("LINOTP", false, null),
	
	// this one is kinda special ...
	CAC("CAC", false, null),
	WINDOWS_USER("WINDOWS_USER", false, null),
	API_USER("API_USER", false, null),
	
	// these are not used as much ...
	TWITTER("TWITTER", true, null),
	DROPBOX("DROPBOX", true, null),
	PRODUCT_HUNT("PRODUCT_HUNT", true, null),
	IN("IN", true, null),

	// catch all for other OAuth
	GENERIC("GENERIC", true, GenericTokenFiller.class.getName()),
	;

	private String label;
	private boolean isOAuth;
	private String tokenFillerClass;
	
	AuthProvider(String label, boolean isOAuth, String tokenFillerClass) {
		this.label = label;
		this.isOAuth = isOAuth;
		this.tokenFillerClass = tokenFillerClass;
	}
	
	public String getLabel() {
		return label;
	}
	
	public boolean isOAuth() {
		return isOAuth;
	}
	
	public String getTokenFillerClass() {
		return tokenFillerClass;
	}
	
	/**
	 * Really gross looking... you get things like "Ms", "Cac"...
	 * IF CREATING NEW LOGIC, PLEASE USE AuthProvider.name / getLabel
	 */
	@Deprecated
	public String toString() {
		return name().charAt(0) + name().substring(1).toLowerCase();
	}
	
	public static AuthProvider getProviderFromString(String authProv) {
		AuthProvider provider = null;
		try {
			provider = AuthProvider.valueOf(authProv.toUpperCase());
		} catch(Exception e){
			provider = AuthProvider.GENERIC;
		}
		return provider;
	}
	
	/**
	 * Get the keys are they should be in the social.properties files
	 * All keys should be the same as the enum name but lower case
	 * @return
	 */
	public static Set<String> getSocialPropKeys() {
		Set<String> vals = new HashSet<>();
		for(AuthProvider auth : AuthProvider.values()) {
			vals.add(auth.name().toLowerCase());
		}
		
		return vals;
	}

}
