package prerna.util.ldap;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.util.SocialPropertiesUtil;

public abstract class AbstractLdapAuthenticator implements ILdapAuthenticator  {

	private static final Logger classLogger = LogManager.getLogger(AbstractLdapAuthenticator.class);

	String providerUrl = null;

	// if multi structure searching
	// need to have an application login to search for the user CN
	// and then try to login as the user
	String applicationSecurityPrincipal = null;
	String applicationSecurityCredentials = null;

	// if single structure searching
	// user is providing the CN to the DN structure
	// and then providing their password
	List<String> securityPrincipalTemplate = null;

	// attribute mapping
	String attributeIdKey = null;
	String attributeNameKey = null;
	String attributeEmailKey = null;
	String attributeUserNameKey = null;

	// if we require password changes
	String attributeLastPwdChangeKey = null;
	int requirePwdChangeAfterDays = 80;
	
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
		String securityPrincipalTemplateStr = socialData.getProperty(LDAP_SECURITY_PRINCIPAL_TEMPLATE);
		if(securityPrincipalTemplateStr != null && 
				securityPrincipalTemplateStr.contains("***")) {
			String[] possibleValues = securityPrincipalTemplateStr.split("\\*\\*\\*");
			securityPrincipalTemplate = Arrays.asList(possibleValues);
		} else {
			securityPrincipalTemplate = new ArrayList<>(1);
			securityPrincipalTemplate.add(securityPrincipalTemplateStr);
		}
		
		this.attributeIdKey = socialData.getProperty(LDAP_ID_KEY);
		this.attributeNameKey = socialData.getProperty(LDAP_NAME_KEY);
		this.attributeEmailKey = socialData.getProperty(LDAP_EMAIL_KEY);
		this.attributeUserNameKey = socialData.getProperty(LDAP_USERNAME_KEY);

		this.attributeLastPwdChangeKey = socialData.getProperty(LDAP_LAST_PWD_CHANGE_KEY);
		String requirePwdChangeDays = socialData.getProperty(LDAP_FORCE_PWD_CHANGE_KEY);
		if(requirePwdChangeDays != null && !requirePwdChangeDays.isEmpty()) {
			try {
				requirePwdChangeAfterDays = Integer.parseInt(requirePwdChangeDays);
			} catch(NumberFormatException e) {
				throw new IllegalArgumentException("Invalid value for " + LDAP_FORCE_PWD_CHANGE_KEY + ". " + e.getMessage());
			}
		}
		
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
		
		List<String> requestAttributesList = new ArrayList<>();
		requestAttributesList.add(this.attributeIdKey);
		if(this.attributeNameKey != null && !this.attributeNameKey.isEmpty()) {
			requestAttributesList.add(this.attributeNameKey);
		}
		if(this.attributeEmailKey != null && !this.attributeEmailKey.isEmpty()) {
			requestAttributesList.add(this.attributeEmailKey);
		}
		if(this.attributeUserNameKey != null && !this.attributeUserNameKey.isEmpty()) {
			requestAttributesList.add(this.attributeUserNameKey);
		}
		if(this.attributeLastPwdChangeKey != null && !this.attributeLastPwdChangeKey.isEmpty()) {
			requestAttributesList.add(this.attributeLastPwdChangeKey);
		}
		
		this.requestAttributes = requestAttributesList.toArray(new String[]{});
	}

	@Override
	public void validate() throws IOException {
		// always need the provider url
		if(this.providerUrl == null || (this.providerUrl=this.providerUrl.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the AD connection URL");
		}
		if(!this.providerUrl.startsWith("ldap://")) {
			this.providerUrl = "ldap://" + this.providerUrl;
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
	
	@Override
	public DirContext createLdapContext(String principalDN, String password) throws Exception {
		return LDAPLoginHelper.createLdapContext(this.providerUrl, principalDN, password);
	}

	@Override
	public AccessToken generateAccessToken(Attributes attributes) throws Exception {
		// for debugging
//		printAllAttributes(attributes);
		
		Object userId = getAttributeValue(attributes, this.attributeIdKey);
		Object name = getAttributeValue(attributes, this.attributeNameKey);
		Object email = getAttributeValue(attributes, this.attributeEmailKey);
		Object username = getAttributeValue(attributes, this.attributeUserNameKey);

		if(userId == null || userId.toString().isEmpty()) {
			throw new IllegalArgumentException("Cannot login user due to not having a proper attribute for the user id");
		}

		if(requirePasswordChange(attributes)) {
			throw new LDAPPasswordChangeRequiredException("User must change their password before login");
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
	
	@Override
	public boolean requirePasswordChange(Attributes attributes) throws NamingException {
		if(this.attributeLastPwdChangeKey != null) {
			// assuming if you define this, that the value must exist
			Object lastPwdChange = getAttributeValue(attributes, this.attributeLastPwdChangeKey);
			if(lastPwdChange == null) {
				throw new IllegalArgumentException("Unable to pull last password change attribute");
			}
			long dateAsLong = 0;
			if(lastPwdChange instanceof Long) {
				dateAsLong = (long) lastPwdChange;
			} else if(lastPwdChange instanceof String) {
				try {
					dateAsLong = Long.parseLong(lastPwdChange+"");
				} catch(NumberFormatException e) {
					throw new IllegalArgumentException("Unable to parse last password change value = '" + lastPwdChange + "'");
				}
			}
			
			LocalDateTime pwdChange = LocalDateTime.ofInstant(Instant.ofEpochMilli(dateAsLong), TimeZone.getTimeZone("UTC").toZoneId()); 			
			LocalDateTime now = LocalDateTime.now();
			if(pwdChange.plusDays(requirePwdChangeAfterDays).isBefore(now)) {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Grab the value of an attribute and perform necessary null checks
	 * @param attributes
	 * @param name
	 * @return
	 * @throws NamingException
	 */
	private Object getAttributeValue(Attributes attributes, String name) throws NamingException {
		if(name == null) {
			return null;
		}
		Attribute attr = attributes.get(name);
		if(attr == null) {
			return null;
		}
		return attr.get();
	}
	
//	/**
//	 * This is for testing - printing all attributes of the logged in user
//	 * @param attributes
//	 * @throws NamingException
//	 */
//	private void printAllAttributes(Attributes attributes) throws NamingException {
//		NamingEnumeration<? extends Attribute> allAttributes = attributes.getAll();
//		while(allAttributes.hasMore()) {
//			Attribute nextAttr = allAttributes.next();
//			if(nextAttr != null) {
//				classLogger.info(nextAttr.getID() + " ::: " + nextAttr.get());
//			}
//		}
//	}

}
