package prerna.util;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Session;
import jakarta.mail.Store;
import prerna.util.ldap.ILdapAuthenticator;
import prerna.util.ldap.LdapAuthenticationFactory;

public class SocialPropertiesUtil {

	private static final Logger classLogger = LogManager.getLogger(SocialPropertiesUtil.class);

	private static SocialPropertiesUtil instance = null;
	private static SocialPropertiesProcessor processor = null;
	private static String socialPropFile = null;
	
	public SocialPropertiesUtil() {
		SocialPropertiesUtil.socialPropFile = DIHelper.getInstance().getProperty(Constants.SOCIAL);
		if(SocialPropertiesUtil.socialPropFile != null) {
			File f = new File(SocialPropertiesUtil.socialPropFile);
			if (!f.exists()) {
				classLogger.warn("No social.properties file found!");
				classLogger.warn("No social.properties file found!");
				classLogger.warn("No social.properties file found!");
			} else {
				SocialPropertiesUtil.processor = new SocialPropertiesProcessor(SocialPropertiesUtil.socialPropFile);
			}
		} else {
			classLogger.warn("No social.properties defined in RDF_Map.prop!");
			classLogger.warn("No social.properties defined in RDF_Map.prop!");
			classLogger.warn("No social.properties defined in RDF_Map.prop!");
		}
	}
	
	public static SocialPropertiesUtil getInstance() {
		if(instance != null) {
			return instance;
		}
		
		synchronized (SocialPropertiesUtil.class) {
			if(instance == null) {
				instance = new SocialPropertiesUtil();
			}
		}
		
		return instance;
	}
	
	public void updateSocialProperties(String provider, Map<String, String> mods) {
		SocialPropertiesUtil.processor.updateProviderProperties(provider, mods);
	}
	
	public void updateAllProperties(String newFileContents) {
		SocialPropertiesUtil.processor.updateAllProperties(newFileContents);
	}
	
	public Map<String, Boolean> getLoginsAllowed() {
		return SocialPropertiesUtil.processor.getLoginsAllowed();
	}
	
	public String getProperty(String key) {
		return SocialPropertiesUtil.processor.getProperty(key);
	}
	
	public String getProperty(String key, String defaultValue) {
		return SocialPropertiesUtil.processor.getProperty(key, defaultValue);
	}
	
	public Object get(Object key) {
		return SocialPropertiesUtil.processor.get(key);
	}
	
	public boolean containsKey(String key) {
		return SocialPropertiesUtil.processor.containsKey(key);
	}
	
	public Set<String> stringPropertyNames() {
		return SocialPropertiesUtil.processor.stringPropertyNames();
	}
	
	public String getLoginRedirect() {
		return SocialPropertiesUtil.processor.getLoginRedirect();
	}
	
	public Map<String, String[]> getSamlAttributeNames() {
		return SocialPropertiesUtil.processor.getSamlAttributeNames();
	}
	
	@Deprecated
	public boolean emailEnabled() {
		classLogger.warn("METHOD DEPRECATED - PLEASE USE smtpEmailEnabled()");
		classLogger.warn("METHOD DEPRECATED - PLEASE USE smtpEmailEnabled()");
		classLogger.warn("METHOD DEPRECATED - PLEASE USE smtpEmailEnabled()");
		classLogger.warn("METHOD DEPRECATED - PLEASE USE smtpEmailEnabled()");
		return smtpEmailEnabled();
	}
	
	// smtp email
	public boolean smtpEmailEnabled() {
		return SocialPropertiesUtil.processor.smtpEmailEnabled();
	}
	
	public String getSmtpUsername() {
		return SocialPropertiesUtil.processor.getSmtpUsername();
	}
	
	public String getSmtpPassword() {
		return SocialPropertiesUtil.processor.getSmtpPassword();
	}
	
	public String getSmtpSender() {
		return SocialPropertiesUtil.processor.getSmtpSender();
	}
	
	public Session getEmailSession() {
		return SocialPropertiesUtil.processor.getSmtpEmailSession();
	}
	
	public Properties getEmailProps() {
		return SocialPropertiesUtil.processor.getSmtpEmailProps();
	}
	
	public Map<String, String> getEmailStaticProps() {
		return SocialPropertiesUtil.processor.getSmtpEmailStaticProps();
	}
	
	// pop3 email
	public boolean pop3EmailEnabled() {
		return SocialPropertiesUtil.processor.pop3EmailEnabled();
	}
	
	public String getPop3Username() {
		return SocialPropertiesUtil.processor.getPop3Username();
	}
	
	public String getPop3Password() {
		return SocialPropertiesUtil.processor.getPop3Password();
	}
	
	public Store getPop3EmailStore() {
		return SocialPropertiesUtil.processor.getPop3EmailStore();
	}
	
	public Properties getPop3EmailProps() {
		return SocialPropertiesUtil.processor.getPop3EmailProps();
	}
	
	// imap email
	public boolean imapEmailEnabled() {
		return SocialPropertiesUtil.processor.imapEmailEnabled();
	}
	
	public String getImapUsername() {
		return SocialPropertiesUtil.processor.getImapUsername();
	}
	
	public String getImapPassword() {
		return SocialPropertiesUtil.processor.getImapPassword();
	}
	
	public Store getImapStore() {
		return SocialPropertiesUtil.processor.getImapEmailStore();
	}
	
	public Properties getImapEmailProps() {
		return SocialPropertiesUtil.processor.getImapEmailProps();
	}
	
	
	public void reloadProps() {
		SocialPropertiesUtil.processor.reloadProps();
	}

	public ILdapAuthenticator getLdapAuthenticator() throws IOException {
		String ldapType = SocialPropertiesUtil.processor.getProperty(ILdapAuthenticator.LDAP_TYPE);
		ILdapAuthenticator ldapAuthenticator = LdapAuthenticationFactory.getAuthenticator(ldapType);
		ldapAuthenticator.load();
		return ldapAuthenticator;
	}

}
