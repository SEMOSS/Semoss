package prerna.testing;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ApiRTestUtils {

	private static final Logger classLogger = LogManager.getLogger(ApiRTestUtils.class);

	public static void setup() throws Exception {
		classLogger.info("Checking and setting up R");
		Path folderInSemoss = Paths.get(ApiTestsSemossConstants.BASE_DIRECTORY, "R");
		if (Files.exists(folderInSemoss)) {
			classLogger.info("R folder in base directory exists, setting up R.");
			copyDirectory();
		} else {
			classLogger.warn("R folder in base directory does not exist, R will NOT be setup.");
		}

	}
	
	private static void copyDirectory() throws Exception {
		Path source = Paths.get(ApiTestsSemossConstants.BASE_DIRECTORY, "R");
		Path test = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "R");

		classLogger.info("SOURCE: {} \n TEST: {}", source, test);
		if (Files.exists(test)) {
			classLogger.info("Test folder exists, cleaning");
			FileUtils.cleanDirectory(test.toFile());
		}

		classLogger.info("Copying source to testfolder");
		FileUtils.copyDirectory(source.toFile(), test.toFile());

	}


}
