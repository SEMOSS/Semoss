package prerna.util.ldap;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.ModificationItem;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.date.SemossDate;
import prerna.util.Constants;

public class LDAPConnectionHelper {
	
	private static final Logger classLogger = LogManager.getLogger(LDAPConnectionHelper.class);

	private transient DirContext ldapContext = null;
	private String principalDN = null;
	private String principalTemplate = null;
	
	private LDAPConnectionHelper() {
		
	}
	
	public DirContext getLdapContext() {
		return ldapContext;
	}
	
	public void setLdapContext(DirContext ldapContext) {
		this.ldapContext = ldapContext;
	}
	
	public String getPrincipalDN() {
		return principalDN;
	}
	
	public void setPrincipalDN(String principalDN) {
		this.principalDN = principalDN;
	}
	
	public String getPrincipalTemplate() {
		return principalTemplate;
	}
	
	public void setPrincipalTemplate(String principalTemplate) {
		this.principalTemplate = principalTemplate;
	}
	
	public static LDAPConnectionHelper tryLogins(String providerUrl, List<String> securityPrincipalTemplate, String username, String password) throws Exception {
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
				classLogger.error("Failed connection with template: " + securityPrincipalTemplate.get(i));
				lastException = e;
			}
			i++;
		} while(i < securityPrincipalTemplate.size());

		// if we dont have any successful login, just throw the last error and also print it to log
		classLogger.error(Constants.STACKTRACE, lastException);
		throw lastException;
	}
	
	public static DirContext createLdapContext(String providerUrl, String principalDN, String password) throws Exception {
		try {
			Properties env = new Properties();
			env.put(Context.INITIAL_CONTEXT_FACTORY, ILdapAuthenticator.LDAP_FACTORY);
			env.put(Context.PROVIDER_URL, providerUrl); // "ldap://localhost:10389";
			env.put(Context.SECURITY_PRINCIPAL, principalDN); // cn=<username>,ou=users,ou=system
			env.put(Context.SECURITY_CREDENTIALS, password); // password
			// specify SSL
			if(providerUrl.startsWith("ldaps")) {
				env.put(Context.SECURITY_PROTOCOL, "ssl");
			}
			
			return new InitialDirContext(env);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}
	
	/**
	 * 
	 * @param windowsFileTime
	 * @return
	 */
	public static LocalDateTime convertWinFileTimeToJava(String windowsFileTime) {
		return convertWinFileTimeToJava(Long.parseLong(windowsFileTime));
	}
	
	/**
	 * 
	 * @param windowsFileTime
	 * @return
	 */
	public static LocalDateTime convertWinFileTimeToJava(long windowsFileTime) {
		// That time is representing 100 nanosecond units since Jan 1. 1601. 
		// There's 116444736000000000 100ns between 1601 and 1970 which is how Java time is stored
		long fixedTime = (windowsFileTime-116444736000000000L) / 10000L;
		LocalDateTime date = LocalDateTime.ofInstant(
				Instant.ofEpochMilli(fixedTime), 
				TimeZone.getTimeZone("UTC").toZoneId()
				);
		return date;
	}
	
	public static AccessToken generateAccessToken(Attributes attributes, 
			String userDN,
			String attributeIdKey, 
			String attributeNameKey, 
			String attributeEmailKey, 
			String attributeUserNameKey,
			String attributeLastPwdChangeKey,
			int requirePwdChangeAfterDays
			) throws Exception {
		// for debugging
//		printAllAttributes(attributes);
		
		Object userId = getAttributeValue(attributes, attributeIdKey);
		Object name = getAttributeValue(attributes, attributeNameKey);
		Object email = getAttributeValue(attributes, attributeEmailKey);
		Object username = getAttributeValue(attributes, attributeUserNameKey);

		if(userId == null || userId.toString().isEmpty()) {
			throw new IllegalArgumentException("Cannot login user due to not having a proper attribute for the user id");
		}

		LocalDateTime lastPwdChange = getLastPwdChange(attributes, attributeLastPwdChangeKey, requirePwdChangeAfterDays);
		
		AccessToken token = new AccessToken();
		token.setProvider(AuthProvider.ACTIVE_DIRECTORY);
		token.setSAN("DN", userDN);
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
		if(lastPwdChange != null) {
			token.setLastPasswordReset(new SemossDate(lastPwdChange));
		}

		return token;
	}
	
	public static LocalDateTime getLastPwdChange(Attributes attributes, String attributeLastPwdChangeKey, int requirePwdChangeAfterDays) throws NamingException {
		// no defined key - nothing to do
		if(attributeLastPwdChangeKey == null || attributeLastPwdChangeKey.isEmpty()) {
			return null;
		}
		
		// assuming if you define this, that the value must exist
		Object lastPwdChange = getAttributeValue(attributes, attributeLastPwdChangeKey);
		if(lastPwdChange == null) {
			throw new IllegalArgumentException("Unable to pull last password change attribute");
		}

		if(requirePwdChangeAfterDays > 0 && lastPwdChange.toString().equals("0")) {
			throw new LDAPPasswordChangeRequiredException("User's last password change is 0, must change their password on first login");
		}
		
		LocalDateTime pwdChange = null;
		if(lastPwdChange instanceof Integer || lastPwdChange instanceof Long) {
			pwdChange = LDAPConnectionHelper.convertWinFileTimeToJava((long) lastPwdChange);
		} else if(lastPwdChange instanceof String) {
			pwdChange = LDAPConnectionHelper.convertWinFileTimeToJava((String) lastPwdChange);
		} else {
			classLogger.warn("Unhandled data type for password change: " + lastPwdChange.getClass().getName());
		}
		
		if(requirePwdChangeAfterDays > 0) {
			if(pwdChange == null) {
				throw new IllegalArgumentException("There is a password change requirement but could not parse last password change attribute, please reach out to an administrator");
			}
			
			if(requirePasswordChange(pwdChange, requirePwdChangeAfterDays)) {
				throw new LDAPPasswordChangeRequiredException("User must change their password before login");
			}
		}

		return pwdChange;
	}
	
	private static boolean requirePasswordChange(LocalDateTime lastPwdChange, int requirePwdChangeAfterDays) throws NamingException {
		LocalDateTime now = LocalDateTime.now();
		if(lastPwdChange.plusDays(requirePwdChangeAfterDays).isBefore(now)) {
			return true;
		}
		return false;
	}
	
	public static void updateUserPassword(String providerUrl, List<String> securityPrincipalTemplate, 
			String username, String curPassword, String newPassword,
			boolean useCustomContextForPwdChange, DirContext customPwdChangeLdapContext) throws Exception { 
		DirContext ldapContext = null;
		String principalDN = null;
		try {
			classLogger.info("Attempting login for user " + username + " to confirm has proper current password");
			LDAPConnectionHelper loginObj = LDAPConnectionHelper.tryLogins(providerUrl, securityPrincipalTemplate, username, curPassword);
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
			if(useCustomContextForPwdChange) {
				if(customPwdChangeLdapContext == null) {
					throw new IllegalArgumentException("Invalid configuration for changing user passwords - please contact your administrator");
				}
				customPwdChangeLdapContext.modifyAttributes(principalDN, mods);
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
	
	/**
	 * Grab the value of an attribute and perform necessary null checks
	 * @param attributes
	 * @param name
	 * @return
	 * @throws NamingException
	 */
	public static Object getAttributeValue(Attributes attributes, String name) throws NamingException {
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
