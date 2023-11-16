package prerna.util.ldap;

import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.TimeZone;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

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
	
	LDAPConnectionHelper() {
		
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
	public static ZonedDateTime convertWinFileTimeToJava(String windowsFileTime) {
		return convertWinFileTimeToJava(Long.parseLong(windowsFileTime));
	}
	
	/**
	 * 
	 * @param windowsFileTime
	 * @return
	 */
	public static ZonedDateTime convertWinFileTimeToJava(long windowsFileTime) {
		// That time is representing 100 nanosecond units since Jan 1. 1601. 
		// There's 116444736000000000 100ns between 1601 and 1970 which is how Java time is stored
		long fixedTime = (windowsFileTime-116444736000000000L) / 10000L;
		return ZonedDateTime.ofInstant(
				Instant.ofEpochMilli(fixedTime), 
				TimeZone.getTimeZone("UTC").toZoneId()
				);
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

		ZonedDateTime lastPwdChange = getLastPwdChange(attributes, attributeLastPwdChangeKey, requirePwdChangeAfterDays);
		
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
	
	public static ZonedDateTime getLastPwdChange(Attributes attributes, String attributeLastPwdChangeKey, int requirePwdChangeAfterDays) throws NamingException {
		// no defined key - nothing to do
		if(attributeLastPwdChangeKey == null || attributeLastPwdChangeKey.isEmpty()) {
			return null;
		}
		
		// assuming if you define this, that the value must exist
		Object lastPwdChange = getAttributeValue(attributes, attributeLastPwdChangeKey);
		if(lastPwdChange == null) {
			throw new IllegalArgumentException("Unable to pull last password change attribute");
		}

		if(lastPwdChange.toString().equals("0")) {
			throw new LDAPPasswordChangeRequiredException("User's last password change is 0, must change their password on first login");
		}
		
		ZonedDateTime pwdChange = null;
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
	
	private static boolean requirePasswordChange(ZonedDateTime lastPwdChange, int requirePwdChangeAfterDays) throws NamingException {
		ZonedDateTime now = ZonedDateTime.now(TimeZone.getTimeZone("UTC").toZoneId());
		if(lastPwdChange.plusDays(requirePwdChangeAfterDays).isBefore(now)) {
			return true;
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
	
	/**
	 * 
	 * @param password
	 * @return
	 * @throws UnsupportedEncodingException 
	 */
	public static byte[] toUnicodePassword(String password) throws UnsupportedEncodingException {
		String quotedPassword = "\"" + password + "\"";
        return quotedPassword.getBytes("UTF-16LE");
        
//		String quotedPassword = "\"" + password + "\"";
//		char unicodePwd[] = quotedPassword.toCharArray();
//		byte pwdArray[] = new byte[unicodePwd.length * 2];
//		for (int i = 0; i < unicodePwd.length; i++)
//		{
//			pwdArray[i * 2 + 1] = (byte) (unicodePwd[i] >>> 8);
//			pwdArray[i * 2 + 0] = (byte) (unicodePwd[i] & 0xff);
//		}
//		return pwdArray;
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
