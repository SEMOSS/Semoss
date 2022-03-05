package prerna.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AuthProvider;

public class SocialPropertiesUtil {

	private static final Logger logger = LogManager.getLogger(SocialPropertiesUtil.class);

	private static SocialPropertiesUtil instance = null;
	private static String socialPropFile = null;
	
	private Properties socialData = null;
	private Map<String, Boolean> loginsAllowedMap;
	
	public static SocialPropertiesUtil getInstance() {
		if(instance != null) {
			return instance;
		}
		
		if(instance == null) {
			instance = new SocialPropertiesUtil();
			instance.loadSocialProperties();
		}
		
		return instance;
	}
	
	private void loadSocialProperties() {
		FileInputStream fis = null;
		SocialPropertiesUtil.socialPropFile = DIHelper.getInstance().getProperty("SOCIAL");
		try {
			if(socialPropFile != null) {
				File f = new File(DIHelper.getInstance().getProperty("SOCIAL"));
				if (f.exists()) {
					this.socialData = new Properties();
					fis = new FileInputStream(f);
					this.socialData.load(fis);
					setLoginsAllowed();
				} else {
					logger.warn("No social.properties file found!");
					logger.warn("No social.properties file found!");
					logger.warn("No social.properties file found!");
				}
			} else {
				logger.warn("No social.properties defined in RDF_Map.prop!");
				logger.warn("No social.properties defined in RDF_Map.prop!");
				logger.warn("No social.properties defined in RDF_Map.prop!");
			}
		} catch (FileNotFoundException fnfe) {
			logger.error(Constants.STACKTRACE, fnfe);
		} catch (IOException ioe) {
			logger.error(Constants.STACKTRACE, ioe);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	private void setLoginsAllowed() {
		this.loginsAllowedMap = new HashMap<>();
		// define the default provider set
		Set<String> defaultProviders = AuthProvider.getSocialPropKeys();
		
		// get all _login props
	    Set<String> loginProps = socialData.stringPropertyNames().stream().filter(str->str.endsWith("_login")).collect(Collectors.toSet());
		for( String prop : loginProps) {
			//prop ex. ms_login
			//get provider from prop by split on _
			String provider = prop.split("_")[0];
		
			this.loginsAllowedMap.put(provider,  Boolean.parseBoolean(socialData.getProperty(prop)));
			//remove the provider from the defaultProvider list
			defaultProviders.remove(provider);
		}
		
		// for loop through the defaultProviders list to make sure we set the rest to false
		for(String provider: defaultProviders) {
			this.loginsAllowedMap.put(provider,  false);
		}

		// get if registration is allowed
		boolean registration = Boolean.parseBoolean(socialData.getProperty("native_registration"));
		this.loginsAllowedMap.put("registration", registration);
	}
	
	public void updateSocialProperties(String provider, Map<String, String> mods) {
		PropertiesConfiguration config = null;
		try {
			config = new PropertiesConfiguration(SocialPropertiesUtil.socialPropFile);
		} catch (ConfigurationException e1) {
			logger.error(Constants.STACKTRACE, e1);
			throw new IllegalArgumentException("An unexpected error happened trying to access the properties. Please try again or reach out to server admin.");
		}

		for (String mod : mods.keySet()) {
			config.setProperty(provider + "_" + mod, mods.get(mod));
		}

		try {
			config.save();
			loadSocialProperties();
		} catch (ConfigurationException e1) {
			throw new IllegalArgumentException("An unexpected error happened when saving the new login properties. Please try again or reach out to server admin.");
		}
	}
	
	public Map<String, Boolean> getLoginsAllowed() {
		return this.loginsAllowedMap;
	}
	
	public String getProperty(String key) {
		return this.socialData.getProperty(key);
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
	
	/**
	 * Return a properties object with the details of the application central SMTP server
	 * @return
	 */
	public Properties getEmailProperties() {
		final String prefix = "smtp_";
		Properties smtpProp = new Properties();
	    Set<String> smtpKeys = this.socialData.stringPropertyNames().stream().filter(str->str.startsWith(prefix)).collect(Collectors.toSet());
	    for(String key : smtpKeys) {
	    	Object smtpValue = socialData.get(key);
	    	if(smtpValue == null) {
	    		continue;
	    	}
	    	// clean up key
	    	String smtpKey = key.replaceFirst(prefix, "").toLowerCase();
	    	smtpProp.put(smtpKey, smtpValue);
	    }
	    if(smtpProp.isEmpty()) {
	    	return null;
	    }
		return smtpProp;
	}
	
}
