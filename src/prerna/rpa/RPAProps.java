package prerna.rpa;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.rpa.security.Cryptographer;
import prerna.rpa.security.EncryptionException;

public class RPAProps {

	private static final Logger LOGGER = LogManager.getLogger(RPAProps.class);
	private static RPAProps instance = null;
	
	// Automatically generated
	public static final String TEXT_DIRECTORY_KEY = "text.directory";
	public static final String JSON_DIRECTORY_KEY = "json.directory";
	
	private static final String ENCRYPTED_REGEX = "<encrypted>(.*?)</encrypted>";
	private final char[] rpaPassword; // For decrypting encrypted properties
	
	// Assumes alphabetical order when saving to file
	private static Properties props = new Properties() {
		private static final long serialVersionUID = 1L;

		@Override
	    public synchronized Enumeration<Object> keys() {
	        return Collections.enumeration(new TreeSet<Object>(super.keySet()));
	    }
	};
	
	private RPAProps() {
		this.rpaPassword = null;
	}

	public static RPAProps getInstance() {
		if(RPAProps.instance != null) {
			return RPAProps.instance;
		}
		
		synchronized(RPAProps.class) {
			if(RPAProps.instance == null) {
				RPAProps.instance = new RPAProps();
			}
		}
		
		return RPAProps.instance;
	}

	public String getProperty(String propertyName) {
		return getProperty(propertyName, "");
	}
	
	public String getProperty(String propertyName, String defaultValue) {
		String propertyValue = props.getProperty(propertyName, defaultValue);
		try {
			propertyValue = decrypt(propertyValue);
		} catch(EncryptionException e) {
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
}
