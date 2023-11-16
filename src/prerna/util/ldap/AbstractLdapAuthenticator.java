package prerna.util.ldap;

import java.time.ZonedDateTime;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;

public abstract class AbstractLdapAuthenticator implements ILdapAuthenticator  {

	private static final Logger classLogger = LogManager.getLogger(AbstractLdapAuthenticator.class);

	@Override
	public DirContext createLdapContext(String providerUrl, String principalDN, String password) throws Exception {
		return LDAPConnectionHelper.createLdapContext(providerUrl, principalDN, password);
	}

	@Override
	public AccessToken generateAccessToken(Attributes attributes, 
			String userDN,
			String attributeIdKey,
			String attributeNameKey,
			String attributeEmailKey, 
			String attributeUserNameKey,
			String attributeLastPwdChangeKey,
			int requirePwdChangeAfterDays
			) throws Exception {
		return LDAPConnectionHelper.generateAccessToken(attributes, userDN, attributeIdKey, attributeNameKey, 
				attributeEmailKey, attributeUserNameKey, 
				attributeLastPwdChangeKey, requirePwdChangeAfterDays);
	}
	
	@Override 
	public ZonedDateTime getLastPwdChange(Attributes attributes, String attributeLastPwdChangeKey, int requirePwdChangeAfterDays) throws NamingException {
		return LDAPConnectionHelper.getLastPwdChange(attributes, attributeLastPwdChangeKey, requirePwdChangeAfterDays);
	}
	
}
