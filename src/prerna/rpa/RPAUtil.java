package prerna.rpa;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RPAUtil {

	private static final Logger logger = LogManager.getLogger(RPAUtil.class.getName());

	private RPAUtil() {
		throw new IllegalStateException("Utility class");
	}
	
	public static long minutesSinceStartTime(long startTimeMillis) {
		return (System.currentTimeMillis() - startTimeMillis)/60000;
	}

	public static long secondsSinceStartTime(long startTimeMillis) {
		return (System.currentTimeMillis() - startTimeMillis)/1000;
	}

	public static String readStringFromFile(String filePath) throws IOException {
		String string;
		try (InputStream in = new FileInputStream(filePath)){
			string = IOUtils.toString(in, "UTF-8");
		} catch (IOException e) {
			logger.error("Failed to read the file " + filePath + ".");
			throw e;
		}
		return string;
	}

}
