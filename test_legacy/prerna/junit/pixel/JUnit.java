package prerna.junit.pixel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.BeforeClass;

public class JUnit {
	
	protected static final Logger LOGGER = LogManager.getLogger(JUnit.class.getName());
	
	protected static final String FILE_SEPARATOR = System.getProperty("file.separator");
	
	protected static final String TEST_RESOURCES_DIRECTORY = new File("test" + FILE_SEPARATOR + "resources").getAbsolutePath(); 

	@BeforeClass
	public static void configureLog4j() {
		String log4JPropertyFile = TEST_RESOURCES_DIRECTORY + FILE_SEPARATOR + "log4j.properties";
		FileInputStream fis = null;
		ConfigurationSource source = null;
		try {
			fis = new FileInputStream(log4JPropertyFile);
			source = new ConfigurationSource(fis);
			Configurator.initialize(null, source);
			LOGGER.info("Successfully configured Log4j for JUnit suite.");
		} catch (IOException e) {
			LOGGER.warn("Unable to initialize log4j for testing.", e);
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
}
