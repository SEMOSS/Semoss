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
public class PixelUnitTests extends PixelUnitWithDatabases {

	protected static final String TESTS_CSV = Paths.get(TEST_RESOURCES_DIRECTORY, "tests.csv").toAbsolutePath().toString();
	
	// Needed for parameterized tests
	private String name;
	private String pixel;
	private String expectedJson;
	private List<String> excludePaths;
	private boolean ignoreOrder;
	private List<String> cleanTestDatabases;
	
	public PixelUnitTests(String name, String pixel, String expectedJson, List<String> excludePaths, boolean ignoreOrder, List<String> cleanTestDatabases) {
		this.name = name;
		this.pixel = pixel;
		this.expectedJson = expectedJson;
		this.excludePaths = excludePaths;
		this.ignoreOrder = ignoreOrder;
		this.cleanTestDatabases = cleanTestDatabases;
	}
		
	@Parameters(name = "{index}: test {0}")
	public static Collection<Object[]> getTestParams() {
		Collection<Object[]> testParams = null;
		try(CSVReader csv = new CSVReader(new FileReader(new File(TESTS_CSV)))) {
			csv.readNext(); // Discard the header
			testParams = csv.readAll().stream().map(row -> new Object[] {
						row[0], // name
						row[1], // pixel
						row[2], // expected
						parseListFromString(row[3]), // exclude paths 
						Boolean.parseBoolean(row[4]), // ignore order
						parseListFromString(row[5]) // clean test databases
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
		LOGGER.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		LOGGER.info("RUNNING TEST: " + name);
		LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		try {
			testPixel(pixel, expectedJson, excludePaths, ignoreOrder);
		} catch (IOException e) {
			LOGGER.error("Error: ", e);
			throw e;
		} finally {
			setCleanTestDatabases(cleanTestDatabases);
		}
	}

}
