package prerna.util.ldap;

public class LdapAuthenticationFactory {
	
	/**
	 * Get the LDAP authentication process
	 * @return
	 */
	public ILdapAuthenticator getAuthenticator() {
		
		
		return new LdapSingleUserStructureConnection();
	}
	
	private LdapAuthenticationFactory() {
		
	}
	
	
	
}
