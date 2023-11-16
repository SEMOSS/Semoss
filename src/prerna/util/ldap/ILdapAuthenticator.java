package prerna.util.ldap;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZonedDateTime;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;

import prerna.auth.AccessToken;

public interface ILdapAuthenticator extends Closeable {

	String LDAP_PASSWORD_CHANGE_RETURN_KEY = "requirePwdChange";
	
	String LDAP_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";
	
	String LDAP = "ldap";
	String LDAP_PREFIX = LDAP + "_";
	
	// what type of ldap process are we using
	String LDAP_TYPE = LDAP_PREFIX + "type";
	String LDAP_TYPE_TEMPLATE = "template";
	String LDAP_TYPE_SEARCH = "search";
	
	// the provider url
	String LDAP_PROVIDER_URL = LDAP_PREFIX + "provider_url";

	// specific to multi structure searching
	String LDAP_APPLICATION_SECURITY_PRINCIPAL = LDAP_PREFIX + "master_principal";
	String LDAP_APPLICATION_SECURITY_CREDENTIALS = LDAP_PREFIX + "master_credentials";
	
	// specific to single structure
	String LDAP_SECURITY_PRINCIPAL_TEMPLATE = LDAP_PREFIX + "principal_tempalte";
	// the key in the template where we will put the user input into the above template
	String SECURITY_PRINCIPAL_TEMPLATE_USERNAME = "<username>";

	// attribute names
	String LDAP_ID_KEY = LDAP_PREFIX + "key_id";
	String LDAP_NAME_KEY = LDAP_PREFIX + "key_name";
	String LDAP_EMAIL_KEY = LDAP_PREFIX + "key_email";
	String LDAP_USERNAME_KEY = LDAP_PREFIX + "key_username";
	
	String LDAP_LAST_PWD_CHANGE_KEY = LDAP_PREFIX + "key_last_pwd_change";
	String LDAP_FORCE_PWD_CHANGE_KEY = LDAP_PREFIX + "require_pwd_change_days";
	String LDAP_USE_CUSTOM_CONTEXT_FOR_PWD_CHANGE_KEY = LDAP_PREFIX + "use_custom_context_for_pwd_change";
	String LDAP_USE_CUSTOM_CONTEXT_FOR_PWD_USERNAME_KEY = LDAP_PREFIX + "use_custom_context_for_pwd_username";
	String LDAP_USE_CUSTOM_CONTEXT_FOR_PWD_PASSWORD_KEY = LDAP_PREFIX + "use_custom_context_for_pwd_password";
	
	// searching
	String LDAP_SEARCH_CONTEXT_NAME = LDAP_PREFIX + "search_context_name";
	String LDAP_SEARCH_CONTEXT_SCOPE = LDAP_PREFIX + "search_context_scope";
	String LDAP_SEARCH_MATCHING_ATTRIBUTES = LDAP_PREFIX + "search_matching_attributes";
	
	/**
	 * Reload properties from social.properties
	 * @throws IOException
	 */
	void load() throws IOException;
	
	/**
	 * Validate the necessary ldap input is provided
	 * @throws IOException
	 */
	void validate() throws IOException;
	
	/**
	 * Create the DirContext for the username/password
	 * @param providerUrl
	 * @param principalDN
	 * @param password
	 * @return
	 * @throws Exception
	 */
	DirContext createLdapContext(String providerUrl, String principalDN, String password) throws Exception;
	
	/**
	 * Authenticate the user input
	 * @param username
	 * @param password
	 * @return
	 */
	AccessToken authenticate(String username, String password) throws Exception;
	
	/**
	 * Produce the access token from the user attributes
	 * @param attributes
	 * @param userDN
	 * @param attributeIdKey
	 * @param attributeNameKey
	 * @param attributeEmailKey
	 * @param attributeUserNameKey
	 * @param attributeLastPwdChangeKey
	 * @param requirePwdChangeAfterDays
	 * @return
	 * @throws Exception
	 */
	AccessToken generateAccessToken(Attributes attributes, 
			String userDN,
			String attributeIdKey, 
			String attributeNameKey, 
			String attributeEmailKey, 
			String attributeUserNameKey,
			String attributeLastPwdChangeKey,
			int requirePwdChangeAfterDays) throws Exception;

	/**
	 * 
	 * @param username
	 * @param curPassword
	 * @param newPassword
	 * @throws Exception
	 */
	void updateUserPassword(String username, String curPassword, String newPassword) throws Exception;

	/**
	 * 
	 * @param username
	 * @param newPassword
	 * @throws Exception
	 */
	void updateForgottenPassword(String username, String newPassword) throws Exception;
	
	/**
	 * 
	 * @param attributes
	 * @param attributeLastPwdChangeKey
	 * @param requirePwdChangeAfterDays
	 * @return
	 * @throws NamingException
	 */
	ZonedDateTime getLastPwdChange(Attributes attributes, String attributeLastPwdChangeKey, int requirePwdChangeAfterDays) throws NamingException;
}
