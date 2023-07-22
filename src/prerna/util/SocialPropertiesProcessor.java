package prerna.util;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Store;
import prerna.auth.AuthProvider;

public class SocialPropertiesProcessor {

	public static final String SMTP_ENABLED = "smtp_enabled";
	public static final String SMTP_USERNAME = "smtp_username";
	public static final String SMTP_PASSWORD = "smtp_password";
	public static final String SMTP_SENDER = "smtp_sender";
	
	public static final String POP3_ENABLED = "pop3_enabled";
	public static final String POP3_USERNAME = "pop3_username";
	public static final String POP3_PASSWORD = "pop3_password";

	public static final String IMAP_ENABLED = "imap_enabled";
	public static final String IMAP_USERNAME = "imap_username";
	public static final String IMAP_PASSWORD = "imap_password";

	private static final Logger logger = LogManager.getLogger(SocialPropertiesProcessor.class);

	private String socialPropFile = null;
	
	// social properties
	private Properties socialData = null;
	private Map<String, Boolean> loginsAllowedMap;
	
	// smtp
	private Session smtpEmailSession = null;
	// pulling out email properties for performance
	private Properties smtpEmailProps = null;
	private Map<String, String> smtpEmailStaticProps = null;

	// pop3
	private Store pop3EmailStore = null;
	// pulling out email properties for performance
	private Properties pop3EmailProps = null;
	
	// imap
	private Store imapEmailStore = null;
	// pulling out email properties for performance
	private Properties imapEmailProps = null;
	
	/**
	 * Constructor
	 * @param socialPropFile
	 */
	public SocialPropertiesProcessor(String socialPropFile) {
		this.socialPropFile = socialPropFile;
		if(this.socialPropFile == null) {
			throw new NullPointerException("Must pass in a social prop file location");
		}
		File f = new File(this.socialPropFile);
		if (!f.exists()) {
			throw new IllegalArgumentException("File does not exist");
		}

		loadSocialProperties();
	}
	
	/**
	 * 
	 */
	public void loadSocialProperties() {
		this.socialData = Utility.loadProperties(this.socialPropFile);
		setLoginsAllowed();
	}
	
	public void setLoginsAllowed() {
		this.loginsAllowedMap = new HashMap<>();
		// define the default provider set
		Set<String> defaultProviders = AuthProvider.getSocialPropKeys();
		
		// get all _login props
	    Set<String> loginProps = socialData.stringPropertyNames().stream().filter(str->str.endsWith("_login")).collect(Collectors.toSet());
		for( String prop : loginProps) {
			//prop ex. ms_login
			//get provider from prop by split on _
			String provider = prop.split("_login")[0];
		
			this.loginsAllowedMap.put(provider,  Boolean.parseBoolean(socialData.getProperty(prop)));
			//remove the provider from the defaultProvider list
			defaultProviders.remove(provider);
		}
		
		// for loop through the defaultProviders list to make sure we set the rest to false
		for(String provider: defaultProviders) {
			this.loginsAllowedMap.put(provider,  false);
		}

		// get if registration is allowed
		boolean registration = Boolean.parseBoolean(socialData.getProperty("native_registration")+"");
		this.loginsAllowedMap.put("registration", registration);
	}
	
	public void updateProviderProperties(String provider, Map<String, String> mods) {
		Map<String, String> updates = new HashMap<>(mods.size());
		for (String mod : mods.keySet()) {
			updates.put(provider + "_" + mod, mods.get(mod));
		}
		updateAllProperties(updates);
	}
	
	public void updateAllProperties(Map<String, String> mods) {
		PropertiesConfiguration config = null;
		try {
			config = new PropertiesConfiguration(this.socialPropFile);
		} catch (ConfigurationException e1) {
			logger.error(Constants.STACKTRACE, e1);
			throw new IllegalArgumentException("An unexpected error happened trying to access the properties. Please try again or reach out to server admin.");
		}

		for (String mod : mods.keySet()) {
			config.setProperty(mod, mods.get(mod));
		}

		try {
			config.save();
			reloadProps();
		} catch (ConfigurationException e1) {
			throw new IllegalArgumentException("An unexpected error happened when saving the new login properties. Please try again or reach out to server admin.");
		}
	}
	
	public void updateAllProperties(String newFileContents) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub

	}
	
	public void reloadProps() {
		// null out values to be reset
		this.loadSocialProperties();
		this.smtpEmailSession = null;
		this.smtpEmailProps = null;
		this.smtpEmailStaticProps = null;
		
		this.pop3EmailProps = null;
		this.pop3EmailStore = null;
		
		this.imapEmailProps = null;
		this.imapEmailStore = null;
	}
	
	public Map<String, Boolean> getLoginsAllowed() {
		return this.loginsAllowedMap;
	}
	
	public String getProperty(String key) {
		return this.socialData.getProperty(key);
	}
	
	public String getProperty(String key, String defaultValue) {
		return this.socialData.getProperty(key, defaultValue);
	}
	
	public Object get(Object key) {
		return this.socialData.get(key);
	}
	
	public boolean containsKey(String key) {
		return this.socialData.containsKey(key);
	}
	
	public Set<String> stringPropertyNames() {
		return this.socialData.stringPropertyNames();
	}
	
	/**
	 * Method to get the redirect URL if defined in the social properties
	 * @return
	 */
	public String getLoginRedirect() {
		String redirectUrl = this.socialData.getProperty("redirect");
		if(redirectUrl.endsWith("#!/login")) {
			return redirectUrl;
		}
		// accounting for some user inputs
		if(!redirectUrl.endsWith("/")) {
			redirectUrl = redirectUrl + "/";
		}
		if(!redirectUrl.endsWith("#!/")) {
			redirectUrl = redirectUrl + "#!/";
		}
		return redirectUrl + "login";
	}
	
	/**
	 * Get the SEMOSS user id to the list of SAML attributes that generate the value
	 * 
	 *			#### SAML Attributes goes here #####
	 *			Rule for specifying the key-values #
	 *			applicationKey - Used by the application. This is specified in the code(See SamlAttributeMapperObject class enum).
	 *			saml_<key_in_saml_assertion> - This is the name of the value specified in AM. Needs to be set here.
	 *			isMandatory - specifies true or false whether the field is required. Need to be set here, either true or false.
	 *			defaultValue - specifies a default value in case a field is not required. This is optional to set.
	 *			Add new key value pairs in the below format.
	 *			saml_<key_in_saml_assertion>	<applicationKey->isMandatory->defaultValue>
	 *			More than 1 mandatory fields can be specified here.
	 * 			
	 * 			Example: 
	 * 
	 * 			saml_id			DOD_EDI_PN_ID
	 *			saml_name		FIRST_NAME+MIDDLE_NAME+LAST_NAME
	 *			saml_email		NULL
	 *			saml_username	FIRST_NAME+MIDDLE_NAME+LAST_NAME
	 * 
	 * @return
	 */
	public Map<String, String[]> getSamlAttributeNames() {
		final String NULL_INPUT = "NULL";
		
		String prefix = Constants.SAML + "_";
		Map<String, String[]> samlAttrMap = new HashMap<>();
	    Set<String> samlProps = this.socialData.stringPropertyNames().stream().filter(str->str.startsWith(prefix)).collect(Collectors.toSet());
	    for(String samlKey : samlProps) {
	    	// key
	    	String socialKey = samlKey.replaceFirst(prefix, "").toLowerCase();
	    	// value
	    	if(socialData.get(samlKey) == null) {
	    		continue;
	    	}
	    	String socialValue = this.socialData.get(samlKey).toString().trim();
	    	if( socialValue.isEmpty() || socialValue.equals(NULL_INPUT)) {
	    		continue;
	    	}
	    	socialValue = socialValue.toLowerCase();
	    	
	    	String[] keyGeneratedBy = socialValue.split("\\+");
			samlAttrMap.putIfAbsent(socialKey, keyGeneratedBy);
	    }
		return samlAttrMap;
	}
	
	public boolean smtpEmailEnabled() {
		return Boolean.parseBoolean(this.socialData.getProperty(SMTP_ENABLED, "false"));
	}
	
	public boolean pop3EmailEnabled() {
		return Boolean.parseBoolean(this.socialData.getProperty(POP3_ENABLED, "false"));
	}
	
	public boolean imapEmailEnabled() {
		return Boolean.parseBoolean(this.socialData.getProperty(IMAP_ENABLED, "false"));
	}
	
	/**
	 * Return a properties object with the details of the application central SMTP server
	 * @return
	 */
	public Properties loadSmtpEmailProperties() {
		final String prefix = "smtp_";
		Properties smtpProp = new Properties();
	    Set<String> smtpKeys = this.socialData.stringPropertyNames().stream().filter(str->str.startsWith(prefix)).collect(Collectors.toSet());
	    for(String key : smtpKeys) {
	    	Object smtpValue = socialData.get(key);
	    	if(smtpValue == null) {
	    		continue;
	    	}
	    	// clean up key
	    	String smtpKey = key.replaceFirst(prefix, "");
	    	smtpProp.put(smtpKey, smtpValue);
	    }
	    if(smtpProp.isEmpty()) {
	    	return null;
	    }
		return smtpProp;
	}
	
	/**
	 * Return a properties object with the details of the application central POP3 server
	 * @return
	 */
	public Properties loadPop3EmailProperties() {
		final String prefix = "pop3_";
		Properties pop3Prop = new Properties();
	    Set<String> smtpKeys = this.socialData.stringPropertyNames().stream().filter(str->str.startsWith(prefix)).collect(Collectors.toSet());
	    for(String key : smtpKeys) {
	    	Object smtpValue = socialData.get(key);
	    	if(smtpValue == null) {
	    		continue;
	    	}
	    	// clean up key
	    	String smtpKey = key.replaceFirst(prefix, "");
	    	pop3Prop.put(smtpKey, smtpValue);
	    }
	    if(pop3Prop.isEmpty()) {
	    	return null;
	    }
		return pop3Prop;
	}
	
	/**
	 * Return a properties object with the details of the application central POP3 server
	 * @return
	 */
	public Properties loadImapEmailProperties() {
		final String prefix = "imap_";
		Properties imapProp = new Properties();
	    Set<String> smtpKeys = this.socialData.stringPropertyNames().stream().filter(str->str.startsWith(prefix)).collect(Collectors.toSet());
	    for(String key : smtpKeys) {
	    	Object smtpValue = socialData.get(key);
	    	if(smtpValue == null) {
	    		continue;
	    	}
	    	// clean up key
	    	String smtpKey = key.replaceFirst(prefix, "");
	    	imapProp.put(smtpKey, smtpValue);
	    }
	    if(imapProp.isEmpty()) {
	    	return null;
	    }
		return imapProp;
	}
	
	/**
	 * Return static properties that can be used to fill in for email templates
	 * @return
	 */
	public Map<String, String> loadSmtpEmailStaticProps() {
		final String prefix = "smtpprop_";
		Map<String, String> emailStaticProps = new HashMap<>();
		Set<String> smtpKeys = this.socialData.stringPropertyNames().stream().filter(str->str.startsWith(prefix)).collect(Collectors.toSet());
		for(String key : smtpKeys) {
			String smtpValue = socialData.getProperty(key);
			if(smtpValue == null) {
				continue;
			}
			// clean up key
			String smtpKey = key.replaceFirst(prefix, "");
			emailStaticProps.put(smtpKey, smtpValue);
		}
		return emailStaticProps;
	}
	
	public void loadSmtpEmailSession() {
		if(this.socialData == null || !smtpEmailEnabled()) {
			return;
		}
		if(this.smtpEmailProps == null || this.smtpEmailProps.isEmpty()) {
			this.smtpEmailProps = loadSmtpEmailProperties();
		}
		if(this.smtpEmailProps == null || this.smtpEmailProps.isEmpty()) {
			throw new IllegalArgumentException("SMTP properties not defined for this instance but it is enabled. Please reach out to an admin to configure");
		}
		this.smtpEmailStaticProps = getSmtpEmailStaticProps();
		
		String username = getSmtpUsername();
		String password = getSmtpPassword();

		try {
			if (username != null && password != null) {
				logger.info("Making secured connection to the email server");
				this.smtpEmailSession = Session.getInstance(this.smtpEmailProps, new jakarta.mail.Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(username, password);
					}
				});
			} else {
				logger.info("Making connection to the email server");
				this.smtpEmailSession = Session.getInstance(this.smtpEmailProps);
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred connecting to the email session defined. Please ensure the proper settings are set for connecting. Detailed error: " + e.getMessage(), e);
		}
	}

	public void loadPop3EmailSession() {
		if(this.socialData == null || !pop3EmailEnabled()) {
			return;
		}
		if(this.pop3EmailProps == null || this.pop3EmailProps.isEmpty()) {
			this.pop3EmailProps = loadPop3EmailProperties();
		}
		if(this.pop3EmailProps == null || this.pop3EmailProps.isEmpty()) {
			throw new IllegalArgumentException("POP3 properties not defined for this instance but it is enabled. Please reach out to an admin to configure");
		}
		this.pop3EmailProps.setProperty("mail.store.protocol", "pop3");

		String host = this.pop3EmailProps.getProperty("mail.pop3.host");
		String username = getPop3Username();
		String password = getPop3Password();

		Session emailSession = null;
		try {
			if (username != null && password != null) {
				logger.info("Making secured connection to the email server");
				emailSession = Session.getInstance(this.pop3EmailProps, new jakarta.mail.Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(username, password);
					}
				});
			} else {
				logger.info("Making connection to the email server");
				emailSession = Session.getInstance(this.pop3EmailProps);
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred connecting to the email session defined. Please ensure the proper settings are set for connecting. Detailed error: " + e.getMessage(), e);
		}

		try {
			//create the POP3 store object and connect with the pop server
			this.pop3EmailStore = emailSession.getStore("pop3s");
			this.pop3EmailStore.connect(host, username, password);
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred establishing the pop3 connection. Please ensure the proper settings are set for connecting. Detailed error: " + e.getMessage(), e);
		}
	}
	
	public void loadImapEmailSession() {
		if(this.socialData == null || !imapEmailEnabled()) {
			return;
		}
		if(this.imapEmailProps == null || this.imapEmailProps.isEmpty()) {
			this.imapEmailProps = loadImapEmailProperties();
		}
		if(this.imapEmailProps == null || this.imapEmailProps.isEmpty()) {
			throw new IllegalArgumentException("IMAP properties not defined for this instance but it is enabled. Please reach out to an admin to configure");
		}
		this.imapEmailProps.setProperty("mail.store.protocol", "imaps");

		String host = this.pop3EmailProps.getProperty("mail.imap.host");
		String username = getImapUsername();
		String password = getImapPassword();

		Session emailSession = null;
		try {
			if (username != null && password != null) {
				logger.info("Making secured connection to the email server");
				emailSession  = Session.getInstance(this.imapEmailProps, new jakarta.mail.Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(username, password);
					}
				});
			} else {
				logger.info("Making connection to the email server");
				emailSession = Session.getInstance(this.imapEmailProps);
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred connecting to the email session defined. Please ensure the proper settings are set for connecting. Detailed error: " + e.getMessage(), e);
		}
		
		try {
			//create the POP3 store object and connect with the pop server
			this.imapEmailStore = emailSession.getStore("imaps");
			this.imapEmailStore.connect(host, username, password);
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred establishing the pop3 connection. Please ensure the proper settings are set for connecting. Detailed error: " + e.getMessage(), e);
		}
	}
	
	public String getSmtpUsername() {
		return this.socialData.getProperty(SMTP_USERNAME);
	}
	
	public String getSmtpPassword() {
		return this.socialData.getProperty(SMTP_PASSWORD);
	}
	
	public String getSmtpSender() {
		return this.socialData.getProperty(SMTP_SENDER);
	}
	
	public String getPop3Username() {
		return this.socialData.getProperty(POP3_USERNAME);
	}
	
	public String getPop3Password() {
		return this.socialData.getProperty(POP3_PASSWORD);
	}
	
	public String getImapUsername() {
		return this.socialData.getProperty(IMAP_USERNAME);
	}
	
	public String getImapPassword() {
		return this.socialData.getProperty(IMAP_PASSWORD);
	}


	public Session getSmtpEmailSession() {
		if(this.smtpEmailSession == null) {
			loadSmtpEmailSession();
		}
		return this.smtpEmailSession;
	}
	
	public Store getPop3EmailStore() {
		if(this.pop3EmailStore == null) {
			loadPop3EmailSession();
		}
		return this.pop3EmailStore;
	}
	
	public Store getImapEmailStore() {
		if(this.imapEmailStore == null) {
			loadImapEmailSession();
		}
		return this.imapEmailStore;
	}
	
	public Properties getSmtpEmailProps() {
		if(this.smtpEmailProps == null) {
			this.smtpEmailProps = loadSmtpEmailProperties();
		}
		if(this.smtpEmailProps == null) {
			return null;
		}
		return new Properties(this.smtpEmailProps);
	}
	
	public Properties getPop3EmailProps() {
		if(this.pop3EmailProps == null) {
			this.pop3EmailProps = loadPop3EmailProperties();
		}
		if(this.pop3EmailProps == null) {
			return null;
		}
		return new Properties(this.pop3EmailProps);
	}
	
	public Properties getImapEmailProps() {
		if(this.imapEmailProps == null) {
			this.imapEmailProps = loadImapEmailProperties();
		}
		if(this.imapEmailProps == null) {
			return null;
		}
		return new Properties(this.imapEmailProps);
	}
	
	public Map<String, String> getSmtpEmailStaticProps() {
		if(this.smtpEmailStaticProps == null) {
			this.smtpEmailStaticProps = loadSmtpEmailStaticProps();
		}
		if(this.smtpEmailStaticProps == null) {
			return null;
		}
		return new HashMap<>(this.smtpEmailStaticProps);
	}
}
