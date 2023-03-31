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
import prerna.auth.AuthProvider;

public class SocialPropertiesProcessor {

	public static final String SMTP_ENABLED = "smtp_enabled";
	public static final String SMTP_USERNAME = "smtp_username";
	public static final String SMTP_PASSWORD = "smtp_password";
	public static final String SMTP_SENDER = "smtp_sender";

	private static final Logger logger = LogManager.getLogger(SocialPropertiesProcessor.class);

	private String socialPropFile = null;
	
	// social properties
	private Properties socialData = null;
	private Map<String, Boolean> loginsAllowedMap;
	
	// email properties
	private Session emailSession = null;
	// pulling out email properties for performance
	private Properties emailProps = null;
	private Map<String, String> emailStaticProps = null;

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
	
	public void reloadProps() {
		// null out values to be reset
		this.loadSocialProperties();
		this.emailSession = null;
		this.emailProps = null;
		this.emailStaticProps = null;
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
	
	public boolean emailEnabled() {
		return Boolean.parseBoolean(this.socialData.getProperty(SMTP_ENABLED, "false"));
	}
	
	/**
	 * Return a properties object with the details of the application central SMTP server
	 * @return
	 */
	public Properties loadEmailProperties() {
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
	 * Return static properties that can be used to fill in for email templates
	 * @return
	 */
	public Map<String, String> loadEmailStaticProps() {
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
	
	public void loadEmailSession() {
		if(this.socialData == null || !emailEnabled()) {
			return;
		}
		if(this.emailProps == null || this.emailProps.isEmpty()) {
			this.emailProps = loadEmailProperties();
		}
		if(this.emailProps == null || this.emailProps.isEmpty()) {
			throw new IllegalArgumentException("SMTP properties not defined for this instance but it is enabled. Please reach out to an admin to configure");
		}
		this.emailStaticProps = getEmailStaticProps();
		
		String username = getSmtpUsername();
		String password = getSmtpPassword();

		try {
			if (username != null && password != null) {
				logger.info("Making secured connection to the email server");
				this.emailSession  = Session.getInstance(this.emailProps, new jakarta.mail.Authenticator() {
					protected PasswordAuthentication getPasswordAuthentication() {
						return new PasswordAuthentication(username, password);
					}
				});
			} else {
				logger.info("Making connection to the email server");
				this.emailSession = Session.getInstance(this.emailProps);
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred connecting to the email session defined. Please ensure the proper settings are set for connecting. Detailed error: " + e.getMessage(), e);
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
	
	public Session getEmailSession() {
		if(this.emailSession == null) {
			loadEmailSession();
		}
		return this.emailSession;
	}
	
	public Properties getEmailProps() {
		if(this.emailProps == null) {
			this.emailProps = loadEmailProperties();
		}
		if(this.emailProps == null) {
			return null;
		}
		return new Properties(this.emailProps);
	}
	
	public Map<String, String> getEmailStaticProps() {
		if(this.emailStaticProps == null) {
			this.emailStaticProps = loadEmailStaticProps();
		}
		if(this.emailStaticProps == null) {
			return null;
		}
		return new HashMap<>(this.emailStaticProps);
	}
}
