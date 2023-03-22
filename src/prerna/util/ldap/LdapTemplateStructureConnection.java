package prerna.util.ldap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.util.Constants;
import prerna.util.SocialPropertiesUtil;

/**
 * Assumes that the authentication for each user has the same DN structure
 * but only the CN will change based on the user input
 */
public class LdapTemplateStructureConnection extends AbstractLdapAuthenticator {

	private static final Logger classLogger = LogManager.getLogger(LdapTemplateStructureConnection.class);

	String providerUrl = null;

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
	
	// do we use a different user/pass for changing pwds
	boolean useCustomContextForPwdChange = false;
	transient DirContext customPwdChangeLdapContext = null;	
	
	String[] requestAttributes = null;

	// searching for user
	String searchContextScopeString = null;
	int searchContextScope = 2;
	String searchMatchingAttributes = null;

	public void load() throws IOException {
		// now load the values in social props
		SocialPropertiesUtil socialData = SocialPropertiesUtil.getInstance();

		this.providerUrl = socialData.getProperty(LDAP_PROVIDER_URL);
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
		

		// validate any inputs and throw errors if invalid
		validate();
	}

	@Override
	public void validate() throws IOException {
		// always need the provider url
		if(this.providerUrl == null || (this.providerUrl=this.providerUrl.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the AD connection URL");
		}
		if(!this.providerUrl.startsWith("ldaps://") && !this.providerUrl.startsWith("ldap://")) {
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
		
		if(this.securityPrincipalTemplate == null || this.securityPrincipalTemplate.isEmpty()) {
			throw new IllegalArgumentException("Must provide the DN template");
		}
		for(String template : this.securityPrincipalTemplate) {
			if(!template.contains(SECURITY_PRINCIPAL_TEMPLATE_USERNAME)) {
				throw new IllegalArgumentException("Must provide the a location to fill the username passed from the user using " + SECURITY_PRINCIPAL_TEMPLATE_USERNAME);
			}
		}
		
		// do we have anything for custom context to change pwds if users are unable to change
		SocialPropertiesUtil socialData = SocialPropertiesUtil.getInstance();
		this.useCustomContextForPwdChange = Boolean.parseBoolean(socialData.getProperty(LDAP_USE_CUSTOM_CONTEXT_FOR_PWD_CHANGE_KEY)+"");
		if(this.useCustomContextForPwdChange) {
			String customUsername = socialData.getProperty(LDAP_USE_CUSTOM_CONTEXT_FOR_PWD_USERNAME_KEY);
			String customPwd = socialData.getProperty(LDAP_USE_CUSTOM_CONTEXT_FOR_PWD_PASSWORD_KEY);
			try {
				this.customPwdChangeLdapContext = LDAPConnectionHelper.createLdapContext(this.providerUrl, customUsername, customPwd);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE + "Unable to login for processing password changes");
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}


	@Override
	public AccessToken authenticate(String username, String password) throws Exception {
		DirContext ldapContext = null;
		try {
			LDAPConnectionHelper loginObj = LDAPConnectionHelper.tryLogins(this.providerUrl, this.securityPrincipalTemplate, username, password);
			String principalTemplate = loginObj.getPrincipalTemplate();
			String principalDN = loginObj.getPrincipalDN();
			ldapContext = loginObj.getLdapContext();

			// need to search for the user who just logged in
			// so that i can grab the attributes
			String searchFilter = this.searchMatchingAttributes;
			searchFilter = searchFilter.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);

			SearchControls controls = new SearchControls();
			controls.setSearchScope(this.searchContextScope);
			controls.setReturningAttributes(this.requestAttributes);

			NamingEnumeration<SearchResult> users = ldapContext.search(principalDN, searchFilter, controls);

			SearchResult result = null;
			while(users.hasMoreElements()) {
				result = users.next();
				// confirm the DN for this user matches the one used to logged in in case the
				// search returns too many people
				if(result.getNameInNamespace().equals(principalDN)) {
					Attributes attr = result.getAttributes();
					return this.generateAccessToken(attr, principalDN, this.attributeIdKey, this.attributeNameKey, this.attributeEmailKey, 
							this.attributeUserNameKey, this.attributeLastPwdChangeKey, this.requirePwdChangeAfterDays);
				}
			}

			return null;
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if(ldapContext != null) {
				ldapContext.close();
			}
		}
	}

	@Override
	public void updateUserPassword(String username, String curPassword, String newPassword) throws NamingException {
		DirContext ldapContext = null;
		String principalDN = null;
		try {
			classLogger.info("Attempting login for user " + username + " to confirm has proper current password");
			LDAPConnectionHelper loginObj = LDAPConnectionHelper.tryLogins(this.providerUrl, this.securityPrincipalTemplate, username, curPassword);
			String principalTemplate = loginObj.getPrincipalTemplate();
			principalDN = loginObj.getPrincipalDN();
			ldapContext = loginObj.getLdapContext();
			classLogger.info("Successful confirmation of current password for user " + principalDN);

			String quotedPassword = "\"" + newPassword + "\"";
			char unicodePwd[] = quotedPassword.toCharArray();
			byte pwdArray[] = new byte[unicodePwd.length * 2];
			for (int i = 0; i < unicodePwd.length; i++)
			{
				pwdArray[i * 2 + 1] = (byte) (unicodePwd[i] >>> 8);
				pwdArray[i * 2 + 0] = (byte) (unicodePwd[i] & 0xff);
			}

			ModificationItem[] mods = new ModificationItem[1];
			mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("UnicodePwd", pwdArray));

			classLogger.info(principalDN + " is attemping to change password");
			if(this.useCustomContextForPwdChange) {
				if(this.customPwdChangeLdapContext == null) {
					throw new IllegalArgumentException("Invalid configuration for changing user passwords - please contact your administrator");
				}
				this.customPwdChangeLdapContext.modifyAttributes(principalDN, mods);
			} else {
				ldapContext.modifyAttributes(principalDN, mods);
			}
			classLogger.info(principalDN + " successfully changed password");
		} catch (Exception e) {
			if(principalDN == null) {
				classLogger.error("User was unable to authenticate with current password, username entered: " + username);
			} else {
				classLogger.info(principalDN + " failed to change password");
			}
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to change password. Error message: " + e.getMessage());
		} finally {
			if(ldapContext != null) {
				ldapContext.close();
			}
		}
	}
	
	@Override
	public void close() {
		// do nothing
	}

}
