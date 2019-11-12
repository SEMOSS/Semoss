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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import au.com.bytecode.opencsv.CSVReader;

@RunWith(Parameterized.class)
public class PixelUnitTests extends PixelUnit {

	protected static final Logger LOGGER = LogManager.getLogger(PixelUnitTests.class.getName());

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
	private boolean ignoreFailure;
	private String insightState;
	private boolean skipTest;
	
	public PixelUnitTests(String name, String pixel, String expectedJson, boolean compareAll, List<String> excludePaths, boolean ignoreOrder, boolean ignoreAddedDictionary, boolean ignoreAddedIterable, List<String> cleanTestDatabases, boolean ignoreFailure, String insightState, boolean skipTest) {
		this.name = name;
		this.pixel = pixel;
		this.expectedJson = expectedJson;
		this.compareAll = compareAll;
		this.excludePaths = excludePaths;
		this.ignoreOrder = ignoreOrder;
		this.ignoreAddedDictionary = ignoreAddedDictionary;
		this.ignoreAddedIterable = ignoreAddedIterable;
		this.cleanTestDatabases = cleanTestDatabases;
		this.ignoreFailure = ignoreFailure;
		this.insightState = insightState;
		this.skipTest = skipTest;
	}
		
	@Parameters(name = "{index}: test {0}")
	public static Collection<Object[]> getTestParams() {
		return getTestParamsMultiple(TESTS_CSV);
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
						parseListFromString(row[8]), // clean test databases
						Boolean.parseBoolean(row[9]) // ignore failure
					}).collect(Collectors.toList());
		} catch (IOException e) {
			LOGGER.error("Error: ", e);
			assumeNoException(e);
		}
		return testParams;
	}
	
	
	public static Collection<Object[]> getTestParamsMultiple(String csvFile) {

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
					parseListFromString(row[8]), // clean testparseListFromString databases
					Boolean.parseBoolean(row[9]), // ignore failure
					row[10], //end of insight
					Boolean.parseBoolean(row[11]) // skip tests
				}).collect(Collectors.toList());
		} catch (IOException e) {
			LOGGER.error("Error: ", e);
			assumeNoException(e);
		}
		LOGGER.info("Params: " + testParams.size());

		return testParams;
	}
	
	private static List<String> parseListFromString(String string) {
		return string.trim().isEmpty() ? new ArrayList<String>() : Arrays.asList(string.split(","));
	}
	
	@Test
	public void runTest() throws IOException {
		if (!skipTest) {
			runTest(this, name, pixel, expectedJson, compareAll, excludePaths, ignoreOrder, ignoreAddedDictionary, ignoreAddedIterable, cleanTestDatabases, ignoreFailure, insightState);
		}
	}

	public static void runTest(PixelUnit testRunner, String name, String pixel, String expectedJson, boolean compareAll, List<String> excludePaths, boolean ignoreOrder, boolean ignoreAddedDictionary, boolean ignoreAddedIterable, List<String> cleanTestDatabases, boolean ignoreFailure) throws IOException {
		LOGGER.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		LOGGER.info("RUNNING TEST: " + name);
		LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
	
		try {
			testRunner.testPixel(pixel, expectedJson, compareAll, excludePaths, ignoreOrder, ignoreAddedDictionary, ignoreAddedIterable);
		} catch (AssertionError e) {
			if (ignoreFailure) {
				throw new AssumptionViolatedException("Ignoring failure of the test: " + name, e);
			} else {
				throw e;
			}
		} finally {
			testRunner.setCleanTestDatabases(cleanTestDatabases);
		}
	}

	public static void runTest(PixelUnit testRunner, String name, String pixel, String expectedJson, boolean compareAll, List<String> excludePaths, boolean ignoreOrder, boolean ignoreAddedDictionary, boolean ignoreAddedIterable, List<String> cleanTestDatabases, boolean ignoreFailure, String insightState) throws IOException {
		LOGGER.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		LOGGER.info("RUNNING TEST: " + name);
		LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
	
		try {
			testRunner.testPixel(pixel, expectedJson, compareAll, excludePaths, ignoreOrder, ignoreAddedDictionary, ignoreAddedIterable);
		} catch (AssertionError e) {
			if (ignoreFailure) {
				throw new AssumptionViolatedException("Ignoring failure of the test: " + name, e);
			} else {
				throw e;
			}
		} finally {
			testRunner.setCleanTestDatabases(cleanTestDatabases);
		}
	}

	//////////////////////////////////////////////////////////////////////
	// Before and after each test method
	//////////////////////////////////////////////////////////////////////
	
	@Override
	@Before
	public void initializeTest() {
		if (!skipTest) {
			if (insightState != null && !(insightState.isEmpty()) && insightState.contains("START")) {
				initializeTest(true);
			} else {
				this.insight = InsightHolder.getInstance().getInsight();
				this.jep = InsightHolder.getInstance().getPy();
			}
		}
	}
	
	@Override
	public void initializeTest(boolean checkAssumptions) {
		initializeInsight();
		initializeJep();
		cleanTestDatabases = new ArrayList<>(); // Reset the list of databases to clean
		if (checkAssumptions) {
			checkTestAssumptions();
		}
	}
	
	@Override
	@After
	public void destroyTest() {
		if(!skipTest) {
			if(insightState!=null && !(insightState.isEmpty()) && insightState.contains("END")) {
			destroyInsight();
			destroyJep();
			cleanTestDatabases();
			} else {
				InsightHolder.getInstance().setInsight(this.insight);
				InsightHolder.getInstance().setPy(this.jep);;
			}
		}
	}
	

}
