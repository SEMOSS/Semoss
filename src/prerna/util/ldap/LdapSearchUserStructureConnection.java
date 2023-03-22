package prerna.util.ldap;

import java.io.IOException;

import javax.naming.NamingEnumeration;
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

/**
 * Assumes that the authentication for each user has the same DN structure
 * but only the CN will change based on the user input
 */
public class LdapSearchUserStructureConnection extends AbstractLdapAuthenticator {

	private static final Logger classLogger = LogManager.getLogger(LdapSearchUserStructureConnection.class);

	@Override
	public void validate() throws IOException {
		super.validate();
		if(this.securityPrincipalTemplate == null || this.securityPrincipalTemplate.isEmpty()) {
			throw new IllegalArgumentException("Must provide the DN template");
		}
		for(String template : this.securityPrincipalTemplate) {
			if(!template.contains(SECURITY_PRINCIPAL_TEMPLATE_USERNAME)) {
				throw new IllegalArgumentException("Must provide the a location to fill the username passed from the user using " + SECURITY_PRINCIPAL_TEMPLATE_USERNAME);
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

			NamingEnumeration<SearchResult> users = ldapContext.search(this.searchContextName, searchFilter, controls);

			SearchResult result = null;
			while(users.hasMoreElements()) {
				result = users.next();
				// confirm the DN for this user matches the one used to logged in in case the
				// search returns too many people
				if(result.getNameInNamespace().equals(principalDN)) {
					Attributes attr = result.getAttributes();
					return this.generateAccessToken(attr);
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
	public void close() {
		// do nothing
	}

	@Override
	public void updateUserPassword(String username, String curPassword, String newPassword) throws Exception { 
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
}
