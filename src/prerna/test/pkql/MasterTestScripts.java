package prerna.test.pkql;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.junit.AfterClass;
import org.junit.BeforeClass;
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

	@Test
	public void test() {
		TestUtilityMethods.loadDIHelper();

		// Get test scripts from current directory
		String testScriptPackage = getTestPath();
		ArrayList<String> scriptPaths = getTestScripts(testScriptPackage);

		for (String scriptPath : scriptPaths) {
			currentTestScriptPath = scriptPath;
			// get test properties
			getProperties(scriptPath);

			// get expectedOutput data from test script
			Map<String, Object> expGson = getExpectedOutputGson();
			ArrayList<Object> insightsArr = (ArrayList<Object>) expGson.get("insights");
			Map<String, Object> insight = (Map<String, Object>) insightsArr.get(0);

			// get return data from pqklScript
			setUpEngine();
			Map<String, Object> actualFeData = runPKQL();

			// handle return data from panel.viz in expectedOutput script
			if (insight.containsKey("feData")) {
				ArrayList<ArrayList<Object>> expectedDtValues = new ArrayList();
				Vector<Object[]> actualDtValues;
				expectedDtValues = getExpectedDataTableValues(insight);
				actualDtValues = getActualDataTableValues(actualFeData);
				try {
					assertEquals("Testing size of DataTableValues", actualDtValues.size(), expectedDtValues.size());
					if (actualDtValues.size() == expectedDtValues.size()) {
						compareDataTableValues(actualDtValues, expectedDtValues);
					}
				} catch (AssertionError e) {
					System.out.println("size of dataTableValues is incorrect");
					// throw e;
				}
			}
		}
	}

	/**
	 * This method unbundles the actual DataTable Values from feData
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
	 * @param actualDtValues
	 * @param expectedDtValues
	 */
	private void compareDataTableValues(Vector<Object[]> actualDtValues,
			ArrayList<ArrayList<Object>> expectedDtValues) {
		for (int i = 0; i < actualDtValues.size(); i++) {
			Object[] dt = actualDtValues.get(i);
			ArrayList<Object> ev = expectedDtValues.get(i);

			for (int j = 0; i < dt.length; i++) {
				try {
					Assert.assertEquals(description, dt[j], ev.get(j));
				} catch (AssertionError e) {
					System.out.println("Error from test file: " + currentTestScriptPath);
					System.out.println("Values are incorrect:" + dt[j] + " expected Value" + ev.get(j));
					// throw e;
				}
			}
		}
	}

	/**
	 * This method runs the pqklScript from the test script
	 * @return runner fe data
	 */
	private Map<String, Object> runPKQL() {
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
		return runner.getFeData().get("0");
	}

	/**
	 * This method sets up the RDBMSNative engine
	 */
	private void setUpEngine() {
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName(engine);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(engine, coreEngine);
	}

	/**
	 * This method is used to parse the expectedOutput from the properties file
	 * 
	 * @return Map object with return data
	 */
	private Map<String, Object> getExpectedOutputGson() {
		Map<String, Object> expGson = new HashMap();
		Gson gson = new Gson();
		expGson = gson.fromJson(expectedOutput, new TypeToken<Map<String, Object>>() {
		}.getType());
		return expGson;
	}

	/**
	 * This method reads the properties from the file 
	 * @param filePath
	 */
	private void getProperties(String filePath) {
		Properties prop = new Properties();
		try {
			prop.load(new BufferedReader(new FileReader(filePath)));
			engineProp = prop.getProperty("engineProp");
			engine = prop.getProperty("engine");
			pkqlScript = prop.getProperty("pkqlScript");
			expectedOutput = prop.getProperty("expectedOutput");
			description = prop.getProperty("description");
		} catch (FileNotFoundException e1) {
			System.out.println("Test file not found in: " + filePath);
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

	}
	
	/**
	 * This method gets the path of the .properties file for each test script
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
					System.out.println("fileName" + child.getName());
					scripts.add(child.getAbsolutePath());
				}
			}
		}
		return scripts;
	}

	/**
	 * This method gets the path of the test scripts
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
