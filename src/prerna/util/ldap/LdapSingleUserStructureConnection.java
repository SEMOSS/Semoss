package prerna.util.ldap;

import java.io.IOException;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;

/**
 * Assumes that the authentication for each user has the same DN structure
 * but only the CN will change based on the user input
 */
public class LdapSingleUserStructureConnection extends AbstractLdapAuthenticator {

	private static final Logger classLogger = LogManager.getLogger(LdapSingleUserStructureConnection.class);
	
	@Override
	public void validate() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public boolean authenticate(String username, String password) throws Exception {
		String principalTemplate = this.securityPrincipalTemplate;
		
		DirContext con = null;
		try {
			Properties env = new Properties();
			env.put(Context.INITIAL_CONTEXT_FACTORY, LDAP_FACTORY);
			env.put(Context.PROVIDER_URL, this.providerUrl); // "ldap://localhost:10389";
			env.put(Context.SECURITY_PRINCIPAL, principalTemplate.replace("<CN>", username)); // cn=<CN>,ou=users,ou=system
			env.put(Context.SECURITY_CREDENTIALS, password); // password
			
			con = new InitialDirContext(env);
			
			return true;
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
