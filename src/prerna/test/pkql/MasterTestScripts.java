package prerna.test.pkql;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import org.junit.Test;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import junit.framework.Assert;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc.PKQLRunner;
import prerna.sablecc.Translation;
import prerna.sablecc.lexer.Lexer;
import prerna.sablecc.lexer.LexerException;
import prerna.sablecc.node.Start;
import prerna.sablecc.parser.Parser;
import prerna.sablecc.parser.ParserException;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;

public class MasterTestScripts {

	// values from *.properties file
	private String description;
	private String pkqlScript;
	private String engineProp;
	private String engine;
	private String expectedOutput;
	private String currentTestScriptPath;
	private ArrayList<String> errors;

	@Test
	public void test() {
		TestUtilityMethods.loadDIHelper();
		errors = new ArrayList<String>();
		// Get test scripts from current directory
		String testScriptPackage = getTestPath();
		ArrayList<String> scriptPaths = new ArrayList<>();
		// Test all Scripts
		scriptPaths = getTestScripts(testScriptPackage);

		// Test one script
		// scriptPaths.add("C:\\Users\\rramirezjimenez\\workspace\\Semoss\\target\\classes\\prerna\\test\\pkql\\data-frame-hasDuplicates-False.properties");

		for (String scriptPath : scriptPaths) {
			System.out.println("Current Script Being Tested: " + scriptPath);

			currentTestScriptPath = scriptPath;
			// get test properties
			getProperties(scriptPath);

			// get expectedOutput data from test script
			Map<String, Object> expGson = getExpectedOutputGson(expectedOutput);
			ArrayList<Object> insightsArr = (ArrayList<Object>) expGson.get("insights");
			Map<String, Object> insight = (Map<String, Object>) insightsArr.get(0);
			ArrayList<Object> expectedPkqlData = (ArrayList<Object>) insight.get("pkqlData");
			Map<String, Object> expectedResultsMap = (Map<String, Object>) expectedPkqlData.get(0);
			String[] pkqlCommands = this.pkqlScript.split(";");
			String command = (String) pkqlCommands[pkqlCommands.length - 1];

			// get return data from pqklScript
			setUpEngine();
			PKQLRunner actualRunner = runPKQL();
			actualRunner.getResults();

			// handle return data from panel.viz in expectedOutput script
			if (command.contains(".viz") || command.contains("col.unfilter") || command.contains("col.filter")) {
				if (insight.containsKey("feData") && ((Map) insight.get("feData")).get("0") != null) {
					ArrayList<ArrayList<Object>> expectedDtValues = new ArrayList();
					Vector<Object[]> actualDtValues;
					expectedDtValues = getExpectedDataTableValues(insight);
					actualDtValues = getActualDataTableValues(actualRunner.getFeData().get("0"));
					try {
						assertEquals("Testing size of DataTableValues", actualDtValues.size(), expectedDtValues.size());
						if (actualDtValues.size() == expectedDtValues.size()) {
							compareDataTableValues(actualDtValues, expectedDtValues);
						}
					} catch (AssertionError e) {
						errors.add("Error in " + currentTestScriptPath);
						errors.add("size of dataTableValues is incorrect");
						// throw e;
					}
				}
			} else if (command.contains("data.frame.getHeaders")) {
				Map<String, Object> returnData = (Map<String, Object>) expectedResultsMap.get("returnData");
				ArrayList<Object> expectedList = (ArrayList<Object>) returnData.get("list");

				List<Map> resultsList = actualRunner.getResults();
				Map<String, Object> actualReturnData = (Map<String, Object>) resultsList.get(3).get("returnData");
				ArrayList<Object> actualList = (ArrayList<Object>) actualReturnData.get("list");

				// Assert.assertEquals(expectedList.toString(),
				// actualList.toString());

				compareArrayList(expectedList, actualList);
			} else if (command.contains("col.filterModel")) {
				Map<String, Object> expectedResults = getExpectedOutputGson((String) expectedResultsMap.get("result"));
				Map<String, Object> expUnfilteredValues = (Map<String, Object>) expectedResults.get("unfilteredValues");
				Map<String, Object> expFilteredValues = (Map<String, Object>) expectedResults.get("filteredValues");
				Map<String, Object> expMinMax = (Map<String, Object>) expectedResults.get("minMax");

				// actual data from pkqlScript
				List<Map> resultsList = actualRunner.getResults();
				Map<String, Object> returnData = (Map<String, Object>) resultsList.get(2).get("returnData");
				Map<String, Object> aUnfilteredValues = (Map<String, Object>) returnData.get("unfilteredValues");
				Map<String, Object> aFilteredValues = (Map<String, Object>) returnData.get("filteredValues");
				Map<String, Object> aMinMax = (Map<String, Object>) returnData.get("minMax");

				// compare Data
				compareMaps(expUnfilteredValues, aUnfilteredValues);
				compareMaps(expFilteredValues, aFilteredValues);
				compareMaps(expMinMax, aMinMax);
			} else if (command.contains(".comment")) {
				Map<String, Object> actualResultsList = actualRunner.getFeData().get("0");
				Map<String, Object> actualComments = (Map<String, Object>) actualResultsList.get("comments");

				Map<String, Object> expectedFeData = (Map<String, Object>) insight.get("feData");
				Map<String, Object> blah = (Map<String, Object>) expectedFeData.get("0");
				Map<String, Object> expectedComments = (Map<String, Object>) blah.get("comments");

				Assert.assertEquals(actualComments, expectedComments);
				// System.out.println("comparing: " + actualComments + " " +
				// expectedComments);

			} else if (command.contains("data.frame.hasDuplicates")) {
				Map<String, Object> results = (Map<String, Object>) expectedPkqlData.get(2);
				List<Map> resultsList = actualRunner.getResults();
				Map<String, Object> actualResults = resultsList.get(2);
				System.out.println("comparing: " + results + "\n " + actualResults);

				Assert.assertEquals(results.toString(), actualResults.toString());
			} else {
				errors.add("Not able to Test : " + scriptPath);
				errors.add("Missing test command: " + command);
				System.out.println("Missing test command: " + command);
			}
		}

		if (errors.size() > 0) {
			logErrors();
		}
	}

	/**
	 * This method compares two ArrayList<Object>
	 * 
	 * @param expectedList
	 * @param actualList
	 */
	private void compareArrayList(ArrayList<Object> expectedList, ArrayList<Object> actualList) {
		if (expectedList.size() == actualList.size()) {
			for (int i = 0; i < expectedList.size() && i < actualList.size(); i++) {
				Assert.assertEquals(expectedList.get(i), actualList.get(i));
				// System.out.println("comparing: " + expectedList.get(i) + " "
				// + actualList.get(i));
			}
		} else {
			errors.add("Unable to compare size not the same: " + this.currentTestScriptPath);
		}
	}

	/**
	 * This method compares the key value pairs from each map
	 * 
	 * @param expectedMap
	 * @param actualMap
	 */
	private void compareMaps(Map<String, Object> expectedMap, Map<String, Object> actualMap) {
		for (String key : expectedMap.keySet()) {
			ArrayList<Object> exp = (ArrayList<Object>) expectedMap.get(key);
			HashSet<Object> actual = (HashSet<Object>) actualMap.get(key);
			Object[] actualArr = actual.toArray();
			if (exp.size() == actual.size()) {
				for (int i = 0; i < exp.size(); i++) {
					assertEquals(exp.get(i), actualArr[i]);
					System.out.println("comparing: " + exp.get(i) + " " + actualArr[i]);
				}
			}
		}
	}

	/**
	 * Logging errors to file
	 */
	private void logErrors() {
		try {
			String errorPath = this.getTestPath() + "\\errors.txt";
			errorPath = errorPath.substring(1, errorPath.length());
			System.out.println("Errors check " + errorPath);
			PrintWriter writer = new PrintWriter(errorPath, "UTF-8");
			for (String e : errors) {
				writer.println(e);
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method unbundles the actual DataTable Values from feData
	 * 
	 * @param actualFeData
	 * @return
	 */
	private Vector<Object[]> getActualDataTableValues(Map<String, Object> actualFeData) {
		Vector<Object[]> actualDtValues;
		Map<String, Object> actualChartData = (Map<String, Object>) actualFeData.get("chartData");
		String actualLayout = (String) actualChartData.get("layout");
		Vector<String> actualDtKeys = (Vector<String>) actualChartData.get("dataTableKeys");
		actualDtValues = (Vector<Object[]>) actualChartData.get("dataTableValues");
		return actualDtValues;
	}

	/**
	 * This method unbundles the expected DataTableValues from fe insight object
	 * 
	 * @param insight
	 * @return expected DataTableValues
	 */
	private ArrayList<ArrayList<Object>> getExpectedDataTableValues(Map<String, Object> insight) {
		ArrayList<ArrayList<Object>> expectedDtValues = new ArrayList();

		Map<String, Object> expectedFeData = (Map<String, Object>) ((Map<Object, Object>) insight.get("feData"))
				.get("0");
		Map<String, Object> expectedChartData = (Map<String, Object>) expectedFeData.get("chartData");
		String expectedLayout = (String) expectedChartData.get("layout");
		ArrayList<String> expectedDtKeys = (ArrayList<String>) expectedChartData.get("dataTableKeys");
		expectedDtValues = (ArrayList<ArrayList<Object>>) expectedChartData.get("dataTableValues");
		return expectedDtValues;
	}

	/**
	 * This method compares DataTable values
	 * 
	 * @param actualDtValues
	 * @param expectedDtValues
	 */
	private void compareDataTableValues(Vector<Object[]> actualDtValues,
			ArrayList<ArrayList<Object>> expectedDtValues) {
		for (int i = 0; i < actualDtValues.size(); i++) {
			Object[] dt = actualDtValues.get(i);
			ArrayList<Object> ev = expectedDtValues.get(i);

			for (int j = 0; j < dt.length; j++) {
				try {

					// compare types casting
					if (dt[j].getClass() == Double.class) {
						double actualD = (double) dt[j];
						double expectedD = (double) ev.get(j);
						Assert.assertEquals(description, actualD, expectedD);

					} else if (dt[j].getClass() == Long.class) {
						Long actualL = (Long) dt[j];
						double actualD = actualL;
						double expectedD = (double) ev.get(j);
						Assert.assertEquals(description, actualD, expectedD);
					} else {
						String actual = (String) dt[j];
						String expected = (String) ev.get(j);
						Assert.assertEquals(description, actual, expected);
					}
				} catch (AssertionError e) {
					errors.add("Error from test file: " + currentTestScriptPath);
					errors.add("Values are incorrect:" + dt[j] + " expected Value" + ev.get(j));
					// throw e;
				}
			}
		}
	}

	/**
	 * This method runs the pqklScript from the test script
	 * 
	 * @return runner fe data
	 */
	private PKQLRunner runPKQL() {
		Parser p = new Parser(
				new Lexer(new PushbackReader(new InputStreamReader(new StringBufferInputStream(pkqlScript)), 1024)));
		// Parse the input.
		Start tree;
		PKQLRunner runner = new PKQLRunner();
		try {
			tree = p.parse();
			runner = new PKQLRunner();
			tree.apply(new Translation(new H2Frame(), runner));
		} catch (ParserException | LexerException | IOException e) {
			System.out.println("Error with pkqlScript: " + pkqlScript);
			e.printStackTrace();
		}
		return runner;
	}

	/**
	 * This method sets up the RDBMSNative engine
	 */
	private void setUpEngine() {
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId(engine);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(engine, coreEngine);
	}

	/**
	 * This method is used to parse a string
	 * 
	 * @param string
	 *            the string to create a map from
	 * @return Map<String, Object>
	 */
	private Map<String, Object> getExpectedOutputGson(String string) {
		Map<String, Object> expGson = new HashMap();
		Gson gson = new Gson();
		expGson = gson.fromJson(string, new TypeToken<Map<String, Object>>() {
		}.getType());
		return expGson;
	}

	/**
	 * This method reads the properties from the file
	 * 
	 * @param filePath
	 */
	private void getProperties(String filePath) {
		Properties prop = new Properties();
		try {
			prop.load(new BufferedReader(new FileReader(filePath)));
			engineProp = prop.getProperty("engineProp").trim();
			engine = prop.getProperty("engine").trim();
			pkqlScript = prop.getProperty("pkqlScript").trim();
			expectedOutput = prop.getProperty("expectedOutput").trim();
			description = prop.getProperty("description").trim();
		} catch (FileNotFoundException e1) {
			System.out.println("Test file not found in: " + filePath);
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}

	/**
	 * This method gets the path of the .properties file for each test script
	 * 
	 * @param directoryLocation
	 * @return scriptPathList
	 */
	private ArrayList<String> getTestScripts(String directoryLocation) {
		ArrayList<String> scripts = new ArrayList<>();
		// run all test script files
		File dir = new File(directoryLocation);
		File[] directoryListing = dir.listFiles();
		if (directoryListing != null) {
			for (File child : directoryListing) {
				String testScript = child.getName();
				if (testScript.endsWith(".properties")) {
					System.out.println("fileName " + child.getName());
					scripts.add(child.getAbsolutePath());
				}
			}
		}
		return scripts;
	}

	/**
	 * This method gets the path of the test scripts
	 * 
	 * @return testScriptPackage location
	 */
	private String getTestPath() {
		String s = getClass().getName();
		int i = s.lastIndexOf(".");
		if (i > -1)
			s = s.substring(i + 1);
		s = s + ".class";
		System.out.println("name " + s);
		String testScriptPackage = this.getClass().getResource(s).getPath();
		testScriptPackage = testScriptPackage.substring(0, testScriptPackage.lastIndexOf("/"));
		return testScriptPackage;
	}
}
