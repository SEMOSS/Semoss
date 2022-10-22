package prerna.util.ldap;

import java.io.IOException;

import javax.naming.directory.DirContext;

import prerna.auth.AccessToken;
import prerna.util.SocialPropertiesUtil;

public abstract class AbstractLdapAuthenticator implements ILdapAuthenticator  {

	String providerUrl = null;
	
	// if multi structure searching
	// need to have an application login to search for the user CN
	// and then try to login as the user
	String applicationSecurityPrincipal = null;
	String applicationSecurityCredentials = null;
	
	// if single structure searching
	// user is providing the CN to the DN structure
	// and then providing their password as is
	String securityPrincipalTemplate = null;
	
	// attribute mapping
	String attributeIdKey = null;
	String attributeNameKey = null;
	String attributeEmailKey = null;
	String attributeUserNameKey = null;
	
	public void load() throws IOException {
		// try to close anything if valid
		close();
		
		// now load the values in social props
		SocialPropertiesUtil socialData = SocialPropertiesUtil.getInstance();
		
		this.providerUrl = socialData.getProperty(LDAP_PROVIDER_URL);
		this.applicationSecurityPrincipal = socialData.getProperty(LDAP_APPLICATION_SECURITY_PRINCIPAL);
		this.applicationSecurityCredentials = socialData.getProperty(LDAP_APPLICATION_SECURITY_CREDENTIALS);
		this.securityPrincipalTemplate = socialData.getProperty(LDAP_SECURITY_PRINCIPAL_TEMPLATE);
		
		this.attributeIdKey = socialData.getProperty(LDAP_ID_KEY);
		this.attributeNameKey = socialData.getProperty(LDAP_NAME_KEY);
		this.attributeEmailKey = socialData.getProperty(LDAP_EMAIL_KEY);
		this.attributeUserNameKey = socialData.getProperty(LDAP_USERNAME_KEY);
		
		validate();
	}
	

	@Override
	public AccessToken generateAccessToken(DirContext con) throws Exception {
		return null;
	}
	
}
