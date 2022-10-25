package prerna.util.ldap;

import java.io.IOException;

import javax.naming.directory.Attributes;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.util.SocialPropertiesUtil;

public abstract class AbstractLdapAuthenticator implements ILdapAuthenticator  {

	String providerUrl = null;
	
	// if multi structure searching
	// need to have an application login to search for the user CN
	// and then try to login as the user
	String applicationSecurityPrincipal = null;
	String applicationSecurityCredentials = null;
	
	// if single structure searching
	// user is providing the CN to the DN structure
	// and then providing their password as is
	String securityPrincipalTemplate = null;
	
	// attribute mapping
	String attributeIdKey = null;
	String attributeNameKey = null;
	String attributeEmailKey = null;
	String attributeUserNameKey = null;
	
	String[] requestAttributes = null;
	
	// searching for user
	String searchContextName = null;
	String searchContextScopeString = null;
	int searchContextScope = 2;
	String searchMatchingAttributes = null;
	
	public void load() throws IOException {
		// try to close anything if valid
		close();
		
		// now load the values in social props
		SocialPropertiesUtil socialData = SocialPropertiesUtil.getInstance();
		
		this.providerUrl = socialData.getProperty(LDAP_PROVIDER_URL);
		this.applicationSecurityPrincipal = socialData.getProperty(LDAP_APPLICATION_SECURITY_PRINCIPAL);
		this.applicationSecurityCredentials = socialData.getProperty(LDAP_APPLICATION_SECURITY_CREDENTIALS);
		this.securityPrincipalTemplate = socialData.getProperty(LDAP_SECURITY_PRINCIPAL_TEMPLATE);
		
		this.attributeIdKey = socialData.getProperty(LDAP_ID_KEY);
		this.attributeNameKey = socialData.getProperty(LDAP_NAME_KEY);
		this.attributeEmailKey = socialData.getProperty(LDAP_EMAIL_KEY);
		this.attributeUserNameKey = socialData.getProperty(LDAP_USERNAME_KEY);
		
		this.requestAttributes = new String[] {this.attributeIdKey, this.attributeNameKey, this.attributeEmailKey, this.attributeUserNameKey};
		
		String LDAP_SEARCH_CONTEXT_NAME = LDAP_PREFIX + "search_context_name";
		String LDAP_SEARCH_CONTEXT_SCOPE = LDAP_PREFIX + "search_context_scope";
		String LDAP_SEARCH_MATCHING_ATTRIBUTES = LDAP_PREFIX + "search_matching_attributes";
		
		this.searchContextName = socialData.getProperty(LDAP_SEARCH_CONTEXT_NAME);
		if(this.searchContextName == null || (this.searchContextName=this.searchContextName.trim()).isEmpty()) {
			this.searchContextName = "ou=users,ou=system";
		}
		// should match integer values of
		// OBJECT_SCOPE, ONELEVEL_SCOPE, SUBTREE_SCOPE
		this.searchContextScopeString = socialData.getProperty(LDAP_SEARCH_CONTEXT_SCOPE);
		if(this.searchContextScopeString == null || (this.searchContextScopeString=this.searchContextScopeString.trim()).isEmpty()) {
			this.searchContextScopeString = "2";
		}
		this.searchMatchingAttributes = socialData.getProperty(LDAP_SEARCH_MATCHING_ATTRIBUTES);
		if(this.searchMatchingAttributes == null || (this.searchMatchingAttributes=this.searchMatchingAttributes.trim()).isEmpty()) {
			this.searchMatchingAttributes = "(objectClass=inetOrgPerson)";
		}
		validate();
	}
	
	@Override
	public void validate() throws IOException {
		// always need the provider url
		if(this.providerUrl == null || (this.providerUrl=this.providerUrl.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the AD connection URL");
		}
		
		// need to at least have the ID
		if(this.attributeIdKey == null || (this.attributeIdKey=this.attributeIdKey.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the attribute for the user id");
		}
		
		try {
			this.searchContextScope = Integer.parseInt(this.searchContextScopeString);
		} catch(NumberFormatException e) {
			throw new IllegalArgumentException("Search Context scope must be of value 0, 1, or 2");
		}
	}
	
	/**
	 * Produce the access token from the user attributes
	 * @param attributes
	 * @return
	 * @throws Exception
	 */
	protected AccessToken generateAccessToken(Attributes attributes) throws Exception {
		Object userId = null;
		Object name = null;
		Object email = null;
		Object username = null;
		
		userId = attributes.get(this.attributeIdKey).get();
		if(userId == null) {
			throw new IllegalArgumentException("Cannot login user due to not having a proper attribute for the user id");
		}
		if(this.attributeNameKey != null) {
			name = attributes.get(this.attributeNameKey).get();
		}
		if(this.attributeEmailKey != null) {
			email = attributes.get(this.attributeEmailKey).get();
		}
		if(this.attributeUserNameKey != null) {
			username = attributes.get(this.attributeUserNameKey).get();
		}
		
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.ACTIVE_DIRECTORY);
		token.setId(userId + "");
		if(name != null) {
			token.setName(name + "");
		}
		if(email != null) {
			token.setEmail(email + "");
		}
		if(username != null) {
			token.setUsername(username + "");
		}
		
		return token;
	}
	
}
