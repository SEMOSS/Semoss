package prerna.junit.pixel;

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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import au.com.bytecode.opencsv.CSVReader;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelStreamUtility;
/**
 * Class is used to auto-generate test responses and output to expected json file.
 * @author ngalt
 */
public class PixelJsonGenerator extends PixelUnit {
	private static final String CLASS_NAME = PixelStreamUtility.class.getName();
	private static final Logger LOGGER = LogManager.getLogger(CLASS_NAME);
	
	protected static final String TESTS_CSV = Paths.get(TEST_RESOURCES_DIRECTORY, "tests.csv").toAbsolutePath().toString();

	@Test
	public void runConsole() {
		String expectedJson;
		String pixel;
		String insightState;
		boolean skipTest;

		// Read in csv
		Collection<Object[]> testParameters = getTestParamsMultiple(TESTS_CSV);

		// Loop through collection and for each pixel run output to file
		for (Object[] workflow : testParameters) {
			pixel = String.valueOf(workflow[1]);
			String updatePixel = pixel.replace("\n", "").replace("\r", "");
			expectedJson = String.valueOf(workflow[2]);
			insightState = String.valueOf(workflow[10]);
			skipTest = (Boolean) workflow[11];
			PixelRunner returnData = null;

			if (!skipTest) {
				if (insightState.equals("START")) {
					initializeTest(false);
				}
				try {
					returnData = runPixel(updatePixel);
				}
				catch (IOException e) {
					LOGGER.info("Failed in running pixel: " + pixel);
					LOGGER.info("Error Message: " + e.getMessage());
				}
				
				if (expectedJson != null) {
					String modifiedJsonPath = expectedJson.replaceAll("<<<text>>>", "");
					String actualJsonPath = modifiedJsonPath.replaceAll("<<</text>>>", "");
					
					File jsonFile = new File(TEST_TEXT_DIRECTORY + File.separatorChar + actualJsonPath);
					// if file doesn't exist create empty file
					try {
						jsonFile.createNewFile();
					} catch (IOException e) {
						LOGGER.info("Failed to create json file: " + jsonFile);
						LOGGER.info("Error Message: " + e.getMessage());
					}
					// Log workflow
					LOGGER.info("<<<<<<<<<<<<<<<<<<<<<<<<<<" + actualJsonPath + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
					// Write data
					PixelStreamUtility.writePixelDataForTest(returnData, jsonFile);
				}
				if (insightState.equals("END")) {
					destroyTest();
				}
			}
		}
	}

	public static Collection<Object[]> getTestParamsMultiple(String csvFile) {

		Collection<Object[]> testParams = null;
		try (CSVReader csv = new CSVReader(new FileReader(new File(csvFile)))) {
			csv.readNext(); // Discard the header
			testParams = csv.readAll().stream().map(row -> new Object[] { row[0], // name
					row[1], // pixel
					row[2], // expected json
					Boolean.parseBoolean(row[3]), // compare all
					parseListFromString(row[4]), // exclude paths
					Boolean.parseBoolean(row[5]), // ignore order
					Boolean.parseBoolean(row[6]), // ignore added dictionary
					Boolean.parseBoolean(row[7]), // ignore added iterable
					parseListFromString(row[8]), // clean testparseListFromString databases
					Boolean.parseBoolean(row[9]), // ignore failure
					row[10], // end of insight
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
}
