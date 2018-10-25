package prerna.rpa;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rpa.security.Cryptographer;
import prerna.rpa.security.EncryptionException;
import prerna.util.DIHelper;

public class RPAProps {

	private static final Logger LOGGER = LogManager.getLogger(RPAProps.class.getName());

	private static final String FILE_SEPARATOR = System.getProperty("file.separator");
	
	////////////////////////////////////////////////////////////
	// Default property names
	
	// Automatically generated
	public static final String RPA_CONFIG_DIRECTORY_KEY = "rpa.config.directory";
	public static final String PROPERTY_FILE_PATH_KEY = "property.file.path";	
	public static final String TEXT_DIRECTORY_KEY = "text.directory";
	public static final String JSON_DIRECTORY_KEY = "json.directory";
	
	// Only if given
	public static final String TRIGGER_NOW = "trigger.now";
	////////////////////////////////////////////////////////////
	
	////////////////////////////////////////////////////////////
	// Default config files and directories
	private static final String PROPERTY_FILE_NAME = "rpa.properties";
	private static final String PASSWORD_FILE_NAME = "rpa.password";
	private static final String TEXT_FOLDER = "text";
	private static final String JSON_FOLDER = "json";
	////////////////////////////////////////////////////////////
	
	private static final String ENCRYPTED_REGEX = "<encrypted>(.*?)</encrypted>";
	
	// Assumes alphabetical order when saving to file
	private static Properties props = new Properties() {
		private static final long serialVersionUID = 1L;

		@Override
	    public synchronized Enumeration<Object> keys() {
	        return Collections.enumeration(new TreeSet<Object>(super.keySet()));
	    }
	};

	private final String propertyFilePath;

	private final char[] rpaPassword; // For decrypting encrypted properties
	
	// Singleton properties class
	// This needs the rpa.config system property defined
	private RPAProps() {
		String rpaConfigDirectory = DIHelper.getInstance().getProperty(RPA_CONFIG_DIRECTORY_KEY); 
		
		// Just in case make sure that it ends with a file separator
		if (rpaConfigDirectory.endsWith("/") || rpaConfigDirectory.endsWith("\\")) {
			rpaConfigDirectory = rpaConfigDirectory.substring(0, rpaConfigDirectory.length() - 1);
		}
		rpaConfigDirectory += FILE_SEPARATOR;

		// Read in the properties
		propertyFilePath = rpaConfigDirectory + PROPERTY_FILE_NAME;
		try (InputStream in = new FileInputStream(propertyFilePath)) {
			props.load(in);
		} catch (IOException e) {
			String noPropsMessage = "Unable to read properties from " + propertyFilePath + ". Check that the "
					+ PROPERTY_FILE_NAME + " file exists in " + rpaConfigDirectory + ".";
			LOGGER.error(noPropsMessage);

			// No props, no point in running this program
			throw new FailedToInitializeException(noPropsMessage, e);
		}

		// Read in the password for decrypting encrypted properties
		// For security, don't store the password plainly as a property
		// Only store the the password file path as a property
		String passwordFilePath = rpaConfigDirectory + PASSWORD_FILE_NAME;
		try (InputStream in = new FileInputStream(passwordFilePath)) {
			rpaPassword = IOUtils.toCharArray(in, "UTF-8");
		} catch (IOException e) {
			String noPasswordMessage = "Unable to read password from " + passwordFilePath + ". Check that the "
					+ PASSWORD_FILE_NAME + " file exists in " + rpaConfigDirectory + ".";
			LOGGER.error(noPasswordMessage);

			// No password, no point in running this program
			throw new FailedToInitializeException(noPasswordMessage, e);
		}
		
		// Set default properties
		setProperty(RPA_CONFIG_DIRECTORY_KEY, rpaConfigDirectory);
		setProperty(PROPERTY_FILE_PATH_KEY, propertyFilePath);
		setProperty(TEXT_DIRECTORY_KEY, rpaConfigDirectory + TEXT_FOLDER + FILE_SEPARATOR);
		setProperty(JSON_DIRECTORY_KEY, rpaConfigDirectory + JSON_FOLDER + FILE_SEPARATOR);		
	}

    private static class LazyHolder {
    	
    	private LazyHolder() {
    		throw new IllegalStateException("Static class");
    	}
    	
        private static final RPAProps INSTANCE = new RPAProps();
    }
	
	public static RPAProps getInstance() {
		return LazyHolder.INSTANCE;
	}

	public String getProperty(String propertyName) {
		return getProperty(propertyName, "");
	}
	
	public String getProperty(String propertyName, String defaultValue) {
		String propertyValue = props.getProperty(propertyName, defaultValue);
		try {
			propertyValue = decrypt(propertyValue);
		} catch(EncryptionException e) {
			LOGGER.error("Failed to decrypt the encrypted property " + propertyName + ". Please check to make sure the password in " + PASSWORD_FILE_NAME + " is correct.");
			throw e;
		}
		return propertyValue;
	}
	
	public void setProperty(String propertyName, String propertyValue) {
		props.put(propertyName, propertyValue);
	}
	
	public void setEncrpytedProperty(String propertyName, String propertyValue) {
		props.put(propertyName, encrypt(propertyValue));		
	}
	
	public String encrypt(String value) {
		String salt = Cryptographer.getSalt();
		String encryptedValue = Cryptographer.encrypt(value, salt, rpaPassword);
		return "<encrypted>" + salt + encryptedValue + "</encrypted>";
	}
	
	public String decrypt(String encryptedValue) {
		Matcher encryptedMatcher = Pattern.compile(ENCRYPTED_REGEX).matcher(encryptedValue);
		if (encryptedMatcher.matches()) {
			
			// If it matches the pattern, then decrypt
			String encryptedString = encryptedMatcher.group(1);
			String salt = encryptedString.substring(0, 10);
			encryptedString = encryptedString.substring(10);
			return Cryptographer.decrypt(encryptedString, salt, rpaPassword);
		} else {
			
			// Else return the original value
			return encryptedValue;
		}
	}

	public void removeProperty(String propertyName) {
		props.remove(propertyName);
	}
	
	public void flushPropertiesToFile() throws IOException {
		try (FileOutputStream out = new FileOutputStream(propertyFilePath)) {
			props.store(out, null);
		} catch (IOException e) {
			LOGGER.error("Failed to flush properties to " + propertyFilePath + ".");
			throw e;
		}
	}
	
}
