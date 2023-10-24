package prerna.util.ldap;

import java.io.IOException;
import java.util.ArrayList;
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
public class LdapSearchUserStructureConnection extends AbstractLdapAuthenticator {

	private static final Logger classLogger = LogManager.getLogger(LdapSearchUserStructureConnection.class);

	String providerUrl = null;

	// if multi structure searching
	// need to have an application login to search for the user CN
	// and then try to login as the user
	String applicationMasterPrincipal = null;
	String applicationMasterCredentials = null;
	transient DirContext applicationContext = null;	

	// attribute mapping
	String attributeIdKey = null;
	String attributeNameKey = null;
	String attributeEmailKey = null;
	String attributeUserNameKey = null;

	// if we require password changes
	String attributeLastPwdChangeKey = null;
	int requirePwdChangeAfterDays = 90;

	// do we use a different user/pass for changing pwds
	boolean useCustomContextForPwdChange = false;
	transient DirContext customPwdChangeLdapContext = null;	

	String[] requestAttributes = null;

	// searching for user
	String searchContextName = null;
	String searchContextScopeString = null;
	int searchContextScope = 2;
	String searchMatchingAttributes = null;

	public void load() throws IOException {

		// now load the values in social props
		SocialPropertiesUtil socialData = SocialPropertiesUtil.getInstance();

		this.providerUrl = socialData.getProperty(LDAP_PROVIDER_URL);
		this.applicationMasterPrincipal = socialData.getProperty(LDAP_APPLICATION_SECURITY_PRINCIPAL);
		this.applicationMasterCredentials = socialData.getProperty(LDAP_APPLICATION_SECURITY_CREDENTIALS);

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
		this.searchContextName = socialData.getProperty(LDAP_SEARCH_CONTEXT_NAME);
		if(this.searchContextName == null || (this.searchContextName=this.searchContextName.trim()).isEmpty()) {
			this.searchContextName = "(&(cn=<username>)(objectClass=inetOrgPerson))";
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

		// we require a valid application context
		if(this.applicationMasterPrincipal == null || (this.applicationMasterPrincipal=this.applicationMasterPrincipal.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the attribute for the application ldap principal");
		}
		if(this.applicationMasterCredentials == null || (this.applicationMasterCredentials=this.applicationMasterCredentials.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the attribute for the application ldap credentials");
		}
		try {
			this.applicationContext = LDAPConnectionHelper.createLdapContext(this.providerUrl, this.applicationMasterPrincipal, this.applicationMasterCredentials);
		} catch (Exception e) {
			throw new IllegalArgumentException("Unable to login to create the application ldap context");
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
		// first, need to find the users DN 
		// then need to validate the password is accurate

		// need to search for the user who just logged in
		// so that i can grab the attributes
		String searchContextName = this.searchContextName.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);
		String searchFilter = this.searchMatchingAttributes.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);

		SearchControls controls = new SearchControls();
		controls.setSearchScope(this.searchContextScope);
		controls.setReturningAttributes(this.requestAttributes);

		NamingEnumeration<SearchResult> findUser = applicationContext.search(searchContextName, searchFilter, controls);

		boolean foundUser = false;
		SearchResult result = null;
		while(findUser.hasMoreElements()) {
			foundUser = true;
			result = findUser.next();
			// confirm the password works for this user
			String userDN = result.getNameInNamespace();
			classLogger.info("Found user DN = " + userDN + " > attemping to login");
			DirContext userConnection = null;
			try {
				// if successful at login, we are good
				userConnection = LDAPConnectionHelper.createLdapContext(this.providerUrl, userDN, password);
				
				Attributes attr = result.getAttributes();
				return this.generateAccessToken(attr, userDN, this.attributeIdKey, this.attributeNameKey, this.attributeEmailKey, 
						this.attributeUserNameKey, this.attributeLastPwdChangeKey, this.requirePwdChangeAfterDays);
			} catch(Exception e) {
				String message = "Incorrect login for '" + userDN + "'. ";
				if(e instanceof NamingException) {
					String possibleExplanation = ((NamingException) e).getExplanation();
					if(possibleExplanation != null && !(possibleExplanation=possibleExplanation.trim()).isEmpty()) {
						message += "Error message explanation: " + possibleExplanation;
					} else {
						message += "Error message = " + e.getMessage();
					}
				} else {
					message += "Error message = " + e.getMessage();
				}
				classLogger.error(message);
				classLogger.error(Constants.STACKTRACE, e);
				continue;
			} finally {
				if(userConnection != null) {
					userConnection.close();
				}
			}
		}

		if(foundUser) {
			// see if we can determine that they cannot signin but actually need to reset their password details
			if(result != null) {
				// this method will throw LDAPPasswordChangeRequiredException if the password needs to be reset to login
				try {
					this.getLastPwdChange(result.getAttributes(), this.attributeLastPwdChangeKey, this.requirePwdChangeAfterDays);
				} catch(LDAPPasswordChangeRequiredException e) {
					throw e;
				} catch(Exception e) {
					classLogger.error("Error occurred seeing if login issue is that user must change password");
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			
			throw new IllegalArgumentException("Found username but invalid credentials to login");
		} else {
			throw new IllegalArgumentException("Unable to find username = " + username);
		}
	}


	@Override
	public void updateUserPassword(String username, String curPassword, String newPassword) throws NamingException {
		// first find the user DN
		String searchContextName = this.searchContextName.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);
		String searchFilter = this.searchMatchingAttributes.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);

		SearchControls controls = new SearchControls();
		controls.setSearchScope(this.searchContextScope);
		controls.setReturningAttributes(this.requestAttributes);

		NamingEnumeration<SearchResult> findUser = this.applicationContext.search(searchContextName, searchFilter, controls);

		String principalDN = null;
		boolean foundUser = false;
		SearchResult result = null;
		while(findUser.hasMoreElements()) {
			foundUser = true;
			result = findUser.next();
			// we got the user DN
			principalDN = result.getNameInNamespace();
			classLogger.info("Found user DN = " + principalDN + " > attemping to login");
		}
		
		if(!foundUser) {
			throw new IllegalArgumentException("Unable to find username = " + username);
		}

		// we do not need to authenticate
		// just hash the old and new so that way we know it is accurate
		// this will allow us to account for expired passwords due or
		// forced password changes on login
		try {
			byte[] oldPwd = LDAPConnectionHelper.toUnicodePassword(curPassword);
			byte[] newPwd = LDAPConnectionHelper.toUnicodePassword(newPassword);
			ModificationItem[] mods = new ModificationItem[2];
			mods[0] = new ModificationItem(DirContext.REMOVE_ATTRIBUTE, new BasicAttribute("unicodePwd", oldPwd));
			mods[1] = new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute("unicodePwd", newPwd));
			classLogger.info(principalDN + " is attemping to change password");
			if(this.useCustomContextForPwdChange) {
				if(this.customPwdChangeLdapContext == null) {
					throw new IllegalArgumentException("Invalid configuration for changing user passwords - please contact your administrator");
				}
				this.customPwdChangeLdapContext.modifyAttributes(principalDN, mods);
			} else {
				this.applicationContext.modifyAttributes(principalDN, mods);
			}
			classLogger.info(principalDN + " successfully changed password");
		} catch (Exception e) {
			String message = "Failed to change password. ";
			if(e instanceof NamingException) {
				String possibleExplanation = ((NamingException) e).getExplanation();
				if(possibleExplanation != null && !(possibleExplanation=possibleExplanation.trim()).isEmpty()) {
					message += "Error message explanation: " + possibleExplanation;
				} else {
					message += "Error message = " + e.getMessage();
				}
			} else {
				message += "Error message = " + e.getMessage();
			}
			
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(message);
		}
	}

	@Override
	public void updateForgottenPassword(String username, String newPassword) throws Exception {
		// first find the user DN
		String searchContextName = this.searchContextName.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);
		String searchFilter = this.searchMatchingAttributes.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);

		SearchControls controls = new SearchControls();
		controls.setSearchScope(this.searchContextScope);
		controls.setReturningAttributes(this.requestAttributes);

		NamingEnumeration<SearchResult> findUser = this.applicationContext.search(searchContextName, searchFilter, controls);

		String principalDN = null;
		boolean foundUser = false;
		SearchResult result = null;
		while(findUser.hasMoreElements()) {
			foundUser = true;
			result = findUser.next();
			// we got the user DN
			principalDN = result.getNameInNamespace();
			classLogger.info("Found user DN = " + principalDN + " > attemping to login");
		}
		
		if(!foundUser) {
			throw new IllegalArgumentException("Unable to find username = " + username);
		}
		
		try {
			byte[] pwd = LDAPConnectionHelper.toUnicodePassword(newPassword);
			ModificationItem[] mods = new ModificationItem[1];
			mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("unicodePwd", pwd));

			classLogger.info(principalDN + " is attemping to change password");
			if(this.useCustomContextForPwdChange) {
				if(this.customPwdChangeLdapContext == null) {
					throw new IllegalArgumentException("Invalid configuration for changing user passwords - please contact your administrator");
				}
				this.customPwdChangeLdapContext.modifyAttributes(principalDN, mods);
			} else {
				this.applicationContext.modifyAttributes(principalDN, mods);
			}
			classLogger.info(principalDN + " successfully changed password");
		} catch (Exception e) {
			String message = "Failed to change password. ";
			if(e instanceof NamingException) {
				String possibleExplanation = ((NamingException) e).getExplanation();
				if(possibleExplanation != null && !(possibleExplanation=possibleExplanation.trim()).isEmpty()) {
					message += "Error message explanation: " + possibleExplanation;
				} else {
					message += "Error message = " + e.getMessage();
				}
			} else {
				message += "Error message = " + e.getMessage();
			}
			
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(message);
		}
	}
	
	@Override
	public void close() {
		try {
			this.applicationContext.close();
		} catch (NamingException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

}
