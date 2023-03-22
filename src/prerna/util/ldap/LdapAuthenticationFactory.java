package prerna.util.ldap;

public class LdapAuthenticationFactory {
	
	/**
	 * Get the LDAP authentication process
	 * @return
	 */
	public static ILdapAuthenticator getAuthenticator(String ldapType) {
		if(ILdapAuthenticator.LDAP_TYPE_SEARCH.equals(ldapType)) {
			return new LdapSearchUserStructureConnection();
		}
		// default to simple case
		return new LdapTemplateStructureConnection();
	}
	
	private LdapAuthenticationFactory() {
		
	}
}
