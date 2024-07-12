package prerna.rpa.config;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.rpa.RPAProps;
import prerna.rpa.RPAUtil;
import prerna.util.Constants;

public class ConfigUtil {
	
	private static final Logger classLogger = LogManager.getLogger(ConfigUtil.class);
		
	private ConfigUtil() {
		throw new IllegalStateException("Utility class");
	}
	
	// Because keys specific to a particular job are in the format Job.class + ".key"
	// This way if the packages change this won't cause too much of a fuss
	public static String getJSONKey(String jobInputKey) {
		if (jobInputKey.contains(".")) {
			return jobInputKey.substring(jobInputKey.lastIndexOf('.') + 1, jobInputKey.length());
		} else {
			return jobInputKey;
		}
	}
	
	// For convenience, uses the default text file directory in rpa-config
	public static String readStringFromTextFile(String textFileName) throws ParseConfigException {
		try {
			String filePath = RPAProps.getInstance().getProperty(RPAProps.TEXT_DIRECTORY_KEY) + textFileName;
			return RPAUtil.readStringFromFile(filePath);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new ParseConfigException("Failed to read text from the file " + textFileName + ".");
		}
	}
	
	// For convenience, uses the default json file directory in rpa-config
	public static String readStringFromJSONFile(String jsonFileName) throws ParseConfigException {
		try {
			String filePath = RPAProps.getInstance().getProperty(RPAProps.JSON_DIRECTORY_KEY) + jsonFileName;
			return RPAUtil.readStringFromFile(filePath);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new ParseConfigException("Failed to read json from the file " + jsonFileName + ".");
		}
	}
	
	// Replace all references to map keys (surrounded by <>) in a string with the value from the map 
	// If the map is null, then just returns the original string
	public static String replaceWithContext(String string, Map<String, Object> contextualData) {
		String stringWithContext = string;
		if (contextualData != null) {
			for (Map.Entry<String, Object> entry : contextualData.entrySet()) {
				stringWithContext = stringWithContext.replaceAll("<" + entry.getKey() + ">", entry.getValue().toString());
			}
		}
		return stringWithContext;
	}
	
}
