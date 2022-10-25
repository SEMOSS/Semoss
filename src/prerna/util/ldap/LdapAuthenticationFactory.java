package prerna.util.ldap;

public class LdapAuthenticationFactory {
	
	/**
	 * Get the LDAP authentication process
	 * @return
	 */
	public static ILdapAuthenticator getAuthenticator(String ldapType) {
		if(ILdapAuthenticator.LDAP_TYPE_MULTI.equals(ldapType)) {
			
		}
		// default to simple case
		return new LdapSingleUserStructureConnection();
	}
	
	private LdapAuthenticationFactory() {
		
	}
}
