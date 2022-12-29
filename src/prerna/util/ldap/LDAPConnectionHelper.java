package prerna.util.ldap;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import javax.naming.Context;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;

public class LDAPConnectionHelper {
	
	private static final Logger classLogger = LogManager.getLogger(LDAPConnectionHelper.class);

	private transient DirContext ldapContext = null;
	private String principalDN = null;
	private String principalTemplate = null;
	
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
	
}
