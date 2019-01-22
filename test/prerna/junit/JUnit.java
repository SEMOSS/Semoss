package prerna.junit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;

public class JUnit {
	
	protected static final Logger LOGGER = LogManager.getLogger(JUnit.class.getName());
	
	protected static final String FILE_SEPARATOR = System.getProperty("file.separator");
	
	protected static final String TEST_RESOURCES_DIRECTORY = new File("test" + FILE_SEPARATOR + "resources").getAbsolutePath(); 

	@BeforeClass
	public static void configureLog4j() {
		String log4JPropertyFile = TEST_RESOURCES_DIRECTORY + FILE_SEPARATOR + "log4j.properties";
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(log4JPropertyFile));
			PropertyConfigurator.configure(prop);
			LOGGER.info("Successfully configured Log4j for JUnit suite.");
		} catch (IOException e) {
			LOGGER.warn("Unable to initialize log4j for testing.", e);
		}
	}
	
}
