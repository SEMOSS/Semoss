package prerna.junit;

import static org.junit.Assume.assumeNoException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import au.com.bytecode.opencsv.CSVReader;

@RunWith(Parameterized.class)
public class PixelUnitTests extends PixelUnit {

	protected static final String TESTS_CSV = Paths.get(TEST_RESOURCES_DIRECTORY, "tests.csv").toAbsolutePath().toString();
	
	// Needed for parameterized tests
	private String name;
	private String pixel;
	private String expectedJson;
	private boolean compareAll;
	private List<String> excludePaths;
	private boolean ignoreOrder;
	private boolean ignoreAddedDictionary;
	private boolean ignoreAddedIterable;
	private List<String> cleanTestDatabases;
	
	public PixelUnitTests(String name, String pixel, String expectedJson, boolean compareAll, List<String> excludePaths, boolean ignoreOrder, boolean ignoreAddedDictionary, boolean ignoreAddedIterable, List<String> cleanTestDatabases) {
		this.name = name;
		this.pixel = pixel;
		this.expectedJson = expectedJson;
		this.compareAll = compareAll;
		this.excludePaths = excludePaths;
		this.ignoreOrder = ignoreOrder;
		this.ignoreAddedDictionary = ignoreAddedDictionary;
		this.ignoreAddedIterable = ignoreAddedIterable;
		this.cleanTestDatabases = cleanTestDatabases;
	}
		
	@Parameters(name = "{index}: test {0}")
	public static Collection<Object[]> getTestParams() {
		return getTestParams(TESTS_CSV);
	}
	
	public static Collection<Object[]> getTestParams(String csvFile) {
		Collection<Object[]> testParams = null;
		try(CSVReader csv = new CSVReader(new FileReader(new File(csvFile)))) {
			csv.readNext(); // Discard the header
			testParams = csv.readAll().stream().map(row -> new Object[] {
						row[0], // name
						row[1], // pixel
						row[2], // expected json
						Boolean.parseBoolean(row[3]), // compare all
						parseListFromString(row[4]), // exclude paths 
						Boolean.parseBoolean(row[5]), // ignore order
						Boolean.parseBoolean(row[6]), // ignore added dictionary
						Boolean.parseBoolean(row[7]), // ignore added iterable
						parseListFromString(row[8]) // clean test databases
					}).collect(Collectors.toList());
		} catch (IOException e) {
			LOGGER.error("Error: ", e);
			assumeNoException(e);
		}
		return testParams;
	}
	
	private static List<String> parseListFromString(String string) {
		return string.trim().isEmpty() ? new ArrayList<String>() : Arrays.asList(string.split(","));
	}
	
	@Test
	public void runTest() throws IOException {
		runTest(this, name, pixel, expectedJson, compareAll, excludePaths, ignoreOrder, ignoreAddedDictionary, ignoreAddedIterable, cleanTestDatabases);
	}
	
	public static void runTest(PixelUnit testRunner, String name, String pixel, String expectedJson, boolean compareAll, List<String> excludePaths, boolean ignoreOrder, boolean ignoreAddedDictionary, boolean ignoreAddedIterable, List<String> cleanTestDatabases) throws IOException {
		LOGGER.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		LOGGER.info("RUNNING TEST: " + name);
		LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		try {
			testRunner.testPixel(pixel, expectedJson, compareAll, excludePaths, ignoreOrder, ignoreAddedDictionary, ignoreAddedIterable);
		} catch (IOException e) {
			LOGGER.error("Error: ", e);
			throw e;
		} finally {
			testRunner.setCleanTestDatabases(cleanTestDatabases);
		}
	}

}
