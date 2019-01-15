package prerna.junit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

public class PixelUnitTest extends PixelUnit {

	protected static final String TESTS_DIRECTORY = Paths.get(TEST_RESOURCES_DIRECTORY, "tests").toAbsolutePath().toString();
	
	private static final String COLLEGE = "unit_test_college";
	private static final String MOVIE = "unit_test_movie";
	
	private static final String METAMODEL_EXTENSION = "_metamodel.txt";
	
	private static final String PIXEL_EXTENSION = "_pixel.txt";
	private static final String EXPECTED_EXTENSION = "_expected.txt";
	private static final String EXCLUDED_EXTENSION = "_excluded.txt";
	
	@Before
	public void checkAssumptions() {
		checkTestDatabase(COLLEGE);
		checkTestDatabase(MOVIE);
	}
	
	public void checkTestDatabase(String alias) {
		String appId = aliasToAppId.get(alias);
		String pixel = "GetDatabaseMetamodel(database=[\"" + appId + "\"]);";
		try {
			String expectedJson = FileUtils.readFileToString(Paths.get(TEST_DATA_DIRECTORY, alias + METAMODEL_EXTENSION).toFile());
			Object result = compareResult(pixel, expectedJson);
			assumeThat(result, is(equalTo(new HashMap<>())));
		} catch (IOException e) {
			LOGGER.error(e);
			fail();
		}
	}
	
	@Test
	public void runTests() {
			
		// List all the files in the test database directory
		String[] fileNames = new File(TESTS_DIRECTORY).list();
		
		// If there are corresponding csv and txt files, then load the test database
		List<String> pixelNames = new ArrayList<>();
		List<String> expectedNames = new ArrayList<>();
		for (String file : fileNames) {
			if (file.endsWith(PIXEL_EXTENSION)) {
				pixelNames.add(file.substring(0, file.length() - PIXEL_EXTENSION.length()));
			} else if (file.endsWith(EXPECTED_EXTENSION)) {
				expectedNames.add(file.substring(0, file.length() - EXPECTED_EXTENSION.length()));
			}
		}
		for (String test : pixelNames) {
			if (expectedNames.contains(test)) {
				runTest(test);
			}
		}
	}
	
	public void runTest(String test) {
		LOGGER.info("RUNNING TEST: " + test);
		try {
			String pixel = FileUtils.readFileToString(Paths.get(TESTS_DIRECTORY, test + PIXEL_EXTENSION).toFile());
			String expectedJson = FileUtils.readFileToString(Paths.get(TESTS_DIRECTORY, test + EXPECTED_EXTENSION).toFile());
			File excludedFile = Paths.get(TESTS_DIRECTORY, test + EXCLUDED_EXTENSION).toFile();
			if (excludedFile.exists()) {
				List<String> excludePaths = FileUtils.readLines(excludedFile);
				testPixel(pixel, expectedJson, excludePaths);
			} else {
				testPixel(pixel, expectedJson);
			}
		} catch (IOException e) {
			LOGGER.error(e);
			fail();
		}
	}

}
