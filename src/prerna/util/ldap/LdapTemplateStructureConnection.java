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
			LDAPConnectionHelper loginObj = tryLogins(this.providerUrl, this.securityPrincipalTemplate, username, password);
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
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);

			// we couldn't authenticate you
			// but maybe you just need a new password on initial login
			if(ldapContext != null && this.useCustomContextForPwdChange) {
				if(this.customPwdChangeLdapContext == null) {
					classLogger.warn("Invalid configuration - using custom context for pwd change set but context wasn't created");
				} else {
					// we need to search for the user to grab his attributes
					// and see if we must change his password

					// first find the user DN
					String searchContextName = this.searchContextName.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);
					String searchFilter = this.searchMatchingAttributes.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);

					SearchControls controls = new SearchControls();
					controls.setSearchScope(this.searchContextScope);
					controls.setReturningAttributes(this.requestAttributes);

					NamingEnumeration<SearchResult> findUser = this.customPwdChangeLdapContext.search(searchContextName, searchFilter, controls);

					String principalDN = null;
					SearchResult result = null;
					while(findUser.hasMoreElements()) {
						result = findUser.next();
						// we got the user DN
						principalDN = result.getNameInNamespace();
						classLogger.info("Found user DN = " + principalDN);
						try {
							// now grab the last pwd change key and throw any LDAPPasswordChangeRequiredException
							this.getLastPwdChange(result.getAttributes(), this.attributeLastPwdChangeKey, this.requirePwdChangeAfterDays);
						} catch(LDAPPasswordChangeRequiredException e2) {
							throw e2;
						} catch(Exception e2) {
							classLogger.error("An error occurred tryign to search for user last pwd change attribute");
							classLogger.error(Constants.STACKTRACE, e2);
						}
					}
				}
			} else {
				throw e;
			}
		} finally {
			if(ldapContext != null) {
				ldapContext.close();
			}
		}
		
		return null;
	}

	private LDAPConnectionHelper tryLogins(String providerUrl, List<String> securityPrincipalTemplate, String username, String password) throws Exception {
		int i = 0;
		Exception lastException = null;
		do {
			try {
				String principalTemplate = securityPrincipalTemplate.get(i);
				String principalDN = principalTemplate.replace(ILdapAuthenticator.SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);
				DirContext ldapContext = createLdapContext(providerUrl, principalDN, password);

				LDAPConnectionHelper retObj = new LDAPConnectionHelper();
				retObj.setLdapContext(ldapContext);
				retObj.setPrincipalDN(principalDN);
				retObj.setPrincipalTemplate(principalTemplate);
				return retObj;
			} catch(Exception e) {
				String message = "Failed connection with template: " + securityPrincipalTemplate.get(i) + ". ";
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
				lastException = e;
			}
			i++;
		} while(i < securityPrincipalTemplate.size());

		// if we dont have any successful login, just throw the last error and also print it to log
		classLogger.error(Constants.STACKTRACE, lastException);
		throw lastException;
	}

	@Override
	public void updateUserPassword(String username, String curPassword, String newPassword) throws NamingException {
		// if we have a dedicated context for changing passwords
		// we dont need to authenticate but will do
		// a remove and then add attribute context
		// this will allow us to also handle when we require the user
		// to reset their password on initial login since a context cannot be created
		if(this.useCustomContextForPwdChange) {
			if(this.customPwdChangeLdapContext == null) {
				throw new IllegalArgumentException("Invalid configuration for changing user passwords - please contact your administrator");
			}

			// first find the user DN
			String searchContextName = this.searchContextName.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);
			String searchFilter = this.searchMatchingAttributes.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);

			SearchControls controls = new SearchControls();
			controls.setSearchScope(this.searchContextScope);
			controls.setReturningAttributes(this.requestAttributes);

			NamingEnumeration<SearchResult> findUser = this.customPwdChangeLdapContext.search(searchContextName, searchFilter, controls);

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
				classLogger.info("Using ldap context for pwd change");
				byte[] oldPwd = LDAPConnectionHelper.toUnicodePassword(curPassword);
				byte[] newPwd = LDAPConnectionHelper.toUnicodePassword(newPassword);
				ModificationItem[] mods = new ModificationItem[2];
				mods[0] = new ModificationItem(DirContext.REMOVE_ATTRIBUTE, new BasicAttribute("unicodePwd", oldPwd));
				mods[1] = new ModificationItem(DirContext.ADD_ATTRIBUTE, new BasicAttribute("unicodePwd", newPwd));
				this.customPwdChangeLdapContext.modifyAttributes(principalDN, mods);
			} catch(Exception e) {
				String message = null;
				if(principalDN == null) {
					message = "Unable to find principal DN for username : " + username + ". ";
				} else {
					message = "Failed to change password. ";
				}
				
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
				throw new IllegalArgumentException(message);
			}
		} else {
			// we have to try to login with the current users details
			// and user their context to replace their current password
			// since we already know they must have the correct password to create the context
			DirContext ldapContext = null;
			String principalDN = null;
			try {
				classLogger.info("Attempting login for user " + username + " to confirm has proper current password");
				LDAPConnectionHelper loginObj = tryLogins(this.providerUrl, this.securityPrincipalTemplate, username, curPassword);
				String principalTemplate = loginObj.getPrincipalTemplate();
				principalDN = loginObj.getPrincipalDN();
				ldapContext = loginObj.getLdapContext();
				classLogger.info("Successful confirmation of current password for user " + principalDN);

				byte[] newPwd = LDAPConnectionHelper.toUnicodePassword(newPassword);
				ModificationItem[] mods = new ModificationItem[1];
				mods[0] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, new BasicAttribute("unicodePwd", newPwd));

				classLogger.info(principalDN + " is attemping to change password");
				ldapContext.modifyAttributes(principalDN, mods);
				classLogger.info(principalDN + " successfully changed password");
			} catch (Exception e) {
				String message = null;
				if(principalDN == null) {
					message = "User was unable to authenticate with current password, username entered: " + username + ". ";
				} else {
					message = "Failed to change password. ";
				}
				
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
				throw new IllegalArgumentException(message);
			} finally {
				if(ldapContext != null) {
					ldapContext.close();
				}
			}
		}
	}

	@Override
	public void updateForgottenPassword(String username, String newPassword) throws Exception {
		if(this.customPwdChangeLdapContext == null) {
			throw new IllegalArgumentException("Invalid configuration for changing user passwords - please contact your administrator");
		}

		// first find the user DN
		String searchContextName = this.searchContextName.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);
		String searchFilter = this.searchMatchingAttributes.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);

		SearchControls controls = new SearchControls();
		controls.setSearchScope(this.searchContextScope);
		controls.setReturningAttributes(this.requestAttributes);

		NamingEnumeration<SearchResult> findUser = this.customPwdChangeLdapContext.search(searchContextName, searchFilter, controls);

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
			this.customPwdChangeLdapContext.modifyAttributes(principalDN, mods);
			classLogger.info(principalDN + " successfully changed password");
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			
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
			
			throw new IllegalArgumentException(message);
		}
	}

	@Override
	public void close() {
		// do nothing
	}

}
