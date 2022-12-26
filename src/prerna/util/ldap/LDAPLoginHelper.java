package prerna.util.ldap;

import java.util.List;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;

public class LDAPLoginHelper {
	
	private static final Logger classLogger = LogManager.getLogger(LDAPLoginHelper.class);

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
	
	public static LDAPLoginHelper tryLogins(String providerUrl, List<String> securityPrincipalTemplate, String username, String password) throws Exception {
		int i = 0;
		Exception lastException = null;
		do {
			try {
				String principalTemplate = securityPrincipalTemplate.get(i);
				String principalDN = principalTemplate.replace(ILdapAuthenticator.SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);
				DirContext ldapContext = createLdapContext(providerUrl, principalDN, password);

				LDAPLoginHelper retObj = new LDAPLoginHelper();
				retObj.setLdapContext(ldapContext);
				retObj.setPrincipalDN(principalDN);
				retObj.setPrincipalTemplate(principalTemplate);
				return retObj;
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				lastException = e;
			}
			i++;
		} while(i < securityPrincipalTemplate.size());

		// if we dont have any successful login, just throw the last error
		throw lastException;
	}
	
	public static DirContext createLdapContext(String providerUrl, String principalDN, String password) throws Exception {
		try {
			Properties env = new Properties();
			env.put(Context.INITIAL_CONTEXT_FACTORY, ILdapAuthenticator.LDAP_FACTORY);
			env.put(Context.PROVIDER_URL, providerUrl); // "ldap://localhost:10389";
			env.put(Context.SECURITY_PRINCIPAL, principalDN); // cn=<username>,ou=users,ou=system
			env.put(Context.SECURITY_CREDENTIALS, password); // password

			return new InitialDirContext(env);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}
	
}
