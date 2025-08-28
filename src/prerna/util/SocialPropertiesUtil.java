/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.util;

import jakarta.mail.Session;
import jakarta.mail.Store;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.AuthProvider;
import prerna.util.ldap.ILdapAuthenticator;
import prerna.util.ldap.LdapAuthenticationFactory;

public class SocialPropertiesUtil {

	private static final Logger classLogger = LogManager.getLogger(SocialPropertiesUtil.class);

	private static SocialPropertiesUtil instance = null;
	private static SocialPropertiesProcessor processor = null;
	private static String socialPropFile = null;

	public SocialPropertiesUtil() {
		SocialPropertiesUtil.socialPropFile = Utility.getDIHelperProperty(Constants.SOCIAL);
		if (SocialPropertiesUtil.socialPropFile != null) {
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
		if (instance != null) {
			return instance;
		}

		synchronized (SocialPropertiesUtil.class) {
			if (instance == null) {
				instance = new SocialPropertiesUtil();
			}
		}

		return instance;
	}

	public void updateSocialProperties(String provider, Map<String, String> mods) throws ConfigurationException {
		SocialPropertiesUtil.processor.updateProviderProperties(provider, mods);
	}

	public void updateAllProperties(String newFileContents) throws IOException {
		SocialPropertiesUtil.processor.updateAllProperties(newFileContents);
	}

	public String getFileContents() throws NullPointerException, IOException {
		return SocialPropertiesUtil.processor.getFileContents();
	}

	public Map<String, Boolean> getLoginsAllowed() {
		return SocialPropertiesUtil.processor.getLoginsAllowed();
	}

	public boolean accessKeysAllowed(AuthProvider provider) {
		return SocialPropertiesUtil.processor.accessKeyAllowed(provider);
	}

	public boolean isNativeRegistrationAllowed() {
		return SocialPropertiesUtil.processor.isNativeRegistrationAllowed();
	}

	public List<Map<String, Object>> getAvailableProviders() {
		return SocialPropertiesUtil.processor.getAvailableProviders();
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
