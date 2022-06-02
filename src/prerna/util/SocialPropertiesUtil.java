package prerna.util;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.mail.Session;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SocialPropertiesUtil {

	private static final Logger logger = LogManager.getLogger(SocialPropertiesUtil.class);

	private static SocialPropertiesUtil instance = null;
	private static SocialPropertiesProcessor processor = null;
	private static String socialPropFile = null;
	
	public SocialPropertiesUtil() {
		SocialPropertiesUtil.socialPropFile = DIHelper.getInstance().getProperty(Constants.SOCIAL);
		if(SocialPropertiesUtil.socialPropFile != null) {
			File f = new File(SocialPropertiesUtil.socialPropFile);
			if (!f.exists()) {
				logger.warn("No social.properties file found!");
				logger.warn("No social.properties file found!");
				logger.warn("No social.properties file found!");
			} else {
				SocialPropertiesUtil.processor = new SocialPropertiesProcessor(SocialPropertiesUtil.socialPropFile);
			}
		} else {
			logger.warn("No social.properties defined in RDF_Map.prop!");
			logger.warn("No social.properties defined in RDF_Map.prop!");
			logger.warn("No social.properties defined in RDF_Map.prop!");
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
	
	public boolean emailEnabled() {
		return SocialPropertiesUtil.processor.emailEnabled();
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
		return SocialPropertiesUtil.processor.getEmailSession();
	}
	
	public Properties getEmailProps() {
		return SocialPropertiesUtil.processor.getEmailProps();
	}
	
	public Map<String, String> getEmailStaticProps() {
		return SocialPropertiesUtil.processor.getEmailStaticProps();
	}
	
}
