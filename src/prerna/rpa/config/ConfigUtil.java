package prerna.rpa.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import prerna.rpa.RPAProps;

public class ConfigUtil {

	private static final Logger LOGGER = LogManager.getLogger(ConfigUtil.class.getName());
		
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
	public static String readStringFromTextFile(String textFileName) throws Exception {
		String filePath = RPAProps.getInstance().getProperty(RPAProps.TEXT_DIRECTORY_KEY) + textFileName;
		return readStringFromFile(filePath);
	}
	
	// For convenience, uses the default json file directory in rpa-config
	public static String readStringFromJSONFile(String jsonFileName) throws Exception {
		String filePath = RPAProps.getInstance().getProperty(RPAProps.JSON_DIRECTORY_KEY) + jsonFileName;
		return readStringFromFile(filePath);
	}
	
	private static String readStringFromFile(String filePath) throws Exception {
		String string;
		try (InputStream in = new FileInputStream(filePath)){
			string = IOUtils.toString(in, "UTF-8");
		} catch (IOException e) {
			LOGGER.error("Failed to read the file " + filePath + ".");
			throw e;
		}
		return string;
	}
	
}
