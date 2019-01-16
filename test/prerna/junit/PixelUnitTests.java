package prerna.junit;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PixelUnitTests extends PixelUnitWithDatabases {

	protected static final String TESTS_DIRECTORY = Paths.get(TEST_RESOURCES_DIRECTORY, "tests").toAbsolutePath().toString();
		
	private static final String PIXEL_EXTENSION = "_pixel.txt";
	private static final String EXPECTED_EXTENSION = "_expected.txt";
	private static final String EXCLUDED_EXTENSION = "_excluded.txt";
	private static final String CLEAN_EXTENSION = "_clean.txt";
	
	// Needed for parameterized tests
	private String test;
	
	public PixelUnitTests(String test) {
		this.test = test;
	}
	
	// TODO >>>timb: replace this with PK's excel stuff eventually
	@Parameters(name = "test {0}") // TODO >>>timb: this should be named
	public static Collection<Object[]> getTestParams() {
		
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
		
		// Stage all the tests
		Collection<Object[]> params = new ArrayList<>();
		for (String test : pixelNames) {
			if (expectedNames.contains(test)) {
				params.add(new Object[] {test});
			}
		}
		return params;
	}
	
	@Test
	public void runTest() {
		LOGGER.info("RUNNING TEST: " + test);
		try {
			
			// The Pixel
			String pixel = FileUtils.readFileToString(Paths.get(TESTS_DIRECTORY, test + PIXEL_EXTENSION).toFile());
			
			// The expected JSON
			String expectedJson = FileUtils.readFileToString(Paths.get(TESTS_DIRECTORY, test + EXPECTED_EXTENSION).toFile());
			
			// These are optional
			// The excluded paths in the JSON
			File excludedFile = Paths.get(TESTS_DIRECTORY, test + EXCLUDED_EXTENSION).toFile();
			if (excludedFile.exists()) {
				List<String> excludePaths = FileUtils.readLines(excludedFile);
				testPixel(pixel, expectedJson, excludePaths);
			} else {
				testPixel(pixel, expectedJson);
			}
			
			// The dbs to clean after the test
			File cleanFile = Paths.get(TESTS_DIRECTORY, test + CLEAN_EXTENSION).toFile();
			if (cleanFile.exists()) {
				List<String> dbs = FileUtils.readLines(cleanFile);
				setCleanTestDatabases(dbs);
			}
		} catch (IOException e) {
			LOGGER.error(e);
			fail();
		}
	}

}
