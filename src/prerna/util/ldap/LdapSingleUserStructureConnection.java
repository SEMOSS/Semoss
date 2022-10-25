package prerna.util.ldap;

import java.io.IOException;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
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
public class LdapSingleUserStructureConnection extends AbstractLdapAuthenticator {

	private static final Logger classLogger = LogManager.getLogger(LdapSingleUserStructureConnection.class);
	
	@Override
	public void validate() throws IOException {
		super.validate();
		if(this.securityPrincipalTemplate == null || (this.securityPrincipalTemplate=this.securityPrincipalTemplate.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must provide the DN template");
		}
		if(!this.securityPrincipalTemplate.contains(SECURITY_PRINCIPAL_TEMPLATE_USERNAME)) {
			throw new IllegalArgumentException("Must provide the a location to fill the username passed from the user using " + SECURITY_PRINCIPAL_TEMPLATE_USERNAME);
		}
	}
	
	@Override
	public AccessToken authenticate(String username, String password) throws Exception {
		DirContext con = null;
		String principalTemplate = this.securityPrincipalTemplate;
		String principalDN = principalTemplate.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);
		try {
			Properties env = new Properties();
			env.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_FACTORY);
			env.put(Context.PROVIDER_URL, this.providerUrl); // "ldap://localhost:10389";
			env.put(Context.SECURITY_PRINCIPAL, principalDN); // cn=<username>,ou=users,ou=system
			env.put(Context.SECURITY_CREDENTIALS, password); // password
			
			con = new InitialDirContext(env);
			
			// need to search for the user who just logged in
			// so that i can grab the attributes
			String searchFilter = this.searchMatchingAttributes;
			searchFilter = searchFilter.replace(SECURITY_PRINCIPAL_TEMPLATE_USERNAME, username);
			
			SearchControls controls = new SearchControls();
			controls.setSearchScope(this.searchContextScope);
			controls.setReturningAttributes(this.requestAttributes);
			
			NamingEnumeration<SearchResult> users = con.search(this.searchContextName, searchFilter, controls);
			
			SearchResult result = null;
			while(users.hasMoreElements()) {
				result = users.next();
				// confirm the DN for this user matches the one used to logged in in case the
				// search returns too many people
				if(result.getNameInNamespace().equals(principalDN)) {
					Attributes attr = result.getAttributes();
					return generateAccessToken(attr);
				}
			}
			
			return null;
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			if(con != null) {
				try {
					con.close();
				} catch (NamingException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public boolean close() {
		return true;
	}
}
