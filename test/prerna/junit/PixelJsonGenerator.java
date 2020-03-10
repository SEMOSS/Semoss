package prerna.junit;

import static org.junit.Assume.assumeNoException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
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
	private static final Logger LOGGER = Logger.getLogger(CLASS_NAME);

	@Test
	public void runConsole() {
		String end = "";
		while(!end.equalsIgnoreCase("end")) {
			try {	
				// Read pixel from tester
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
	
				LOGGER.info("Enter location of csv file (pixels in csv must be separated by ; and on one line): ");
				String csvLocation = reader.readLine();   
				csvLocation = csvLocation.trim();
				String expectedJson;
				String pixel;
				String insightState;
				boolean skipTest;
	
				// read in csv
				Collection<Object[]> testParameters = getTestParamsMultiple(csvLocation);
				
				//loop through collection and for each pixel run output to file
				for(Object[] workflow: testParameters) {
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
						returnData = runPixel(updatePixel);
						if (expectedJson != null && expectedJson.contains("workflow")) {
							String modifiedJsonPath = expectedJson.replaceAll("<<<text>>>", "");
							String actualJsonPath = modifiedJsonPath.replaceAll("<<</text>>>", "");
							File jsonFile = new File("C:/workspace/Semoss_Dev/test/resources/text/" + actualJsonPath);
							// if file doesn't exist create empty file
							jsonFile.createNewFile();
							// log workflow
							LOGGER.info("<<<<<<<<<<<<<<<<<<<<<<<<<<" + actualJsonPath + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
							// write data
							PixelStreamUtility.writePixelDataForTest(returnData, jsonFile); 
						}
						if (insightState.equals("END")) {
							destroyTest();
						}
					}
				}
			} catch (IOException e) {
				System.out.println("Failed in running pixel" + e.getMessage());
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
