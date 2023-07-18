package prerna.project.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.Session;
import jakarta.mail.Store;
import prerna.util.Constants;
import prerna.util.SocialPropertiesProcessor;

public class ProjectProperties {
	
	private static final Logger classLogger = LogManager.getLogger(ProjectProperties.class);
	private static final String ADMIN_DIRECTORY = ".admin";
	
	private String projectDirString = null;
	private File adminDir = null;
	private File socialProp = null;
	private SocialPropertiesProcessor processor = null;

	public ProjectProperties(String projectDirString, String projectName, String projectId) {
		this.projectDirString = projectDirString;
		this.adminDir = new File(this.projectDirString + "/" + ADMIN_DIRECTORY);
		if(!this.adminDir.exists() || !this.adminDir.isDirectory()) {
			this.adminDir.mkdirs();
		}
		
		String socialPropertiesFileLoc = this.adminDir.getAbsolutePath() + "/" + Constants.SOCIAL_PROPERTIES_FILENAME;
		this.socialProp = new File(socialPropertiesFileLoc);
		if(!this.socialProp.exists() || !this.socialProp.isFile()) {
			try {
				this.socialProp.createNewFile();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		this.processor = new SocialPropertiesProcessor(socialPropertiesFileLoc);
	}
	
	public void updateProviderProperties(String provider, Map<String, String> mods) {
		this.processor.updateProviderProperties(provider, mods);
	}
	
	public void updateAllProperties(Map<String, String> mods) {
		this.processor.updateAllProperties(mods);
	}
	
	public Map<String, Boolean> getLoginsAllowed() {
		return this.processor.getLoginsAllowed();
	}
	
	public String getProperty(String key) {
		return this.processor.getProperty(key);
	}
	
	public Object get(Object key) {
		return this.processor.get(key);
	}
	
	public boolean containsKey(String key) {
		return this.processor.containsKey(key);
	}
	
	public Set<String> stringPropertyNames() {
		return this.processor.stringPropertyNames();
	}
	
	public Map<String, String[]> getSamlAttributeNames() {
		return this.processor.getSamlAttributeNames();
	}
	
	public boolean emailEnabled() {
		return this.processor.smtpEmailEnabled();
	}
	
	public boolean pop3EmailEnabled() {
		return this.processor.pop3EmailEnabled();
	}
	
	public boolean imapEmailEnabled() {
		return this.processor.imapEmailEnabled();
	}
	
	public String getSmtpSender() {
		return this.processor.getSmtpSender();
	}
	
	@Deprecated
	public Session getEmailSession() {
		classLogger.warn("METHOD DEPRECATED - PLEASE USE getSmtpEmailSession()");
		classLogger.warn("METHOD DEPRECATED - PLEASE USE getSmtpEmailSession()");
		classLogger.warn("METHOD DEPRECATED - PLEASE USE getSmtpEmailSession()");
		classLogger.warn("METHOD DEPRECATED - PLEASE USE getSmtpEmailSession()");
		return getSmtpEmailSession();
	}
	
	public Session getSmtpEmailSession() {
		return this.processor.getSmtpEmailSession();
	}
	
	public Store getPop3EmailStore() {
		return this.processor.getPop3EmailStore();
	}
	
	public Store getImapEmailStore() {
		return this.processor.getImapEmailStore();
	}
	
	public Map<String, String> getEmailStaticProps() {
		return this.processor.getSmtpEmailStaticProps();
	}
	
	public void reloadProps() {
		this.processor.reloadProps();
	}
	
	public File getSocialProp() {
		return socialProp;
	}

}
