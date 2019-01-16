package prerna.test;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.excel.XlsDataSet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelStreamUtility;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.reactor.frame.py.PyExecutorThread;
import prerna.util.AbstractFileWatcher;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.google.common.io.Files;
import com.ibm.icu.util.StringTokenizer;

public class QSTest {

	XlsDataSet xl = null;
	
	// the main folder for test folder
	static String testBaseFolder = null;

	String testFileName = null;
	
	static PyExecutorThread py = null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
		// need to locate the RDF Map
		// load the DIHelper
		
		// load all of the engines / load subset of engines
		
		// Initiate the R and Python tester classes to use the frame

		// Load data into a particular data frame
		
		// get a particualr engine
		
		// Need some way to assert if the selections are working right
		// Then the summarizations

		// need basic selections - needs to be sorted for us to compare
		// need simple group by calculations
		// need correlation, regression and other routines for machine learning
		// need to test filter - numeric, alphabetic, less and more etc. 
		
		// need to test this for every frame
	
		// need to test all the required packages are present on python etc. 
		
		
		try
		{
			loadRDF("c:/users/pkapaleeswaran/workspacej3/MonolithDev4/RDF_Map_web.prop");
			catalogEngines();
			testBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/test";
			py = new PyExecutorThread();
			py.start();
			
			
		}catch(Exception ex)
		{
			// nothing can be done here if it comes to it
		}
		
	}
	
	// loading the xl file for input
	// the xl file typically broken into <testname>-input and <testname>-output and optionally <testname>-data
	// the input is usually a pixel - think of it as an insight
	// the output can be one of three things
	// columns - it will only verify if the specified columns are there - row 0 is columns and row 1 - columns names
	// table - the whole table needs to be compared. If this option, this needs to be pushed into a csv for comparison. unless we write the entire logic in java. This is the default option
	// row 1 - specifies the file location. If not specified, the default convention is the location of this xl file / <testname> (directory) / <testname>-expected.csv
	// custom - this is the custom option - you implement your custom logic

	public void loadTestScipt()
	{
		try
		{
			
			xl = new XlsDataSet(new File(testBaseFolder + "/" + testFileName));
			runScript("from deepdiff import DeepDiff");
			runScript("import json");
		
		/*// print the xl data
		String [] tables = xl.getTableNames();
	
		for(int tableIndex =0;tableIndex < tables.length;tableIndex++)
			System.out.println("table > " + tables[tableIndex]);
		
		ITable table = xl.getTable("Movie");
		System.out.println(table.getRowCount() + " <<>> " + table.getTableMetaData().getColumns().length);
		*/
		}catch (Exception ex)
		{
			System.out.println("Can't run this test, the script file is not available");
			System.out.println(ex);
		}
	}
	
	private static void loadRDF(String rdfPropFile)
	{
		System.out.println("Changing file separator value to: '/'");
		System.setProperty("file.separator", "/");
		
		//Load RDF_Map.prop file
		System.out.println("Loading RDF_Map.prop: " + rdfPropFile);
		DIHelper.getInstance().loadCoreProp(rdfPropFile);
	}
	
	public static void catalogEngines() {
		String watcherStr = DIHelper.getInstance().getProperty(Constants.ENGINE_WEB_WATCHER);
		StringTokenizer watchers = new StringTokenizer(watcherStr, ";");
		try {		
			while(watchers.hasMoreElements()) {
				Object monitor = new Object();
				String watcher = watchers.nextToken();
				String watcherClass = DIHelper.getInstance().getProperty(watcher);
				String folder = DIHelper.getInstance().getProperty(watcher + "_DIR");
				String ext = DIHelper.getInstance().getProperty(watcher + "_EXT");
				AbstractFileWatcher watcherInstance = (AbstractFileWatcher) Class.forName(watcherClass).getConstructor(null).newInstance(null);

				watcherInstance.setMonitor(monitor);
				watcherInstance.setFolderToWatch(folder);
				watcherInstance.setExtension(ext);
				watcherInstance.init();
				synchronized(monitor)
				{
					//Thread thread = new Thread(watcherInstance);
					//thread.start();
					watcherInstance.shutdown();
					watcherInstance.run();
					
					//watcherList.add(watcherInstance);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}


	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}
	
	// load and give you a particular engine
	public IEngine loadEngine(String engineID)
	{
		IEngine retEngine = null;
		
		return retEngine;
	}

	@Before
	public void setUp() throws Exception {
		
		// need someway to say what is the main tests it is running
		// We will use testFileName variable to be set

		testFileName = "Basic/Book1.xlsx";
		loadTestScipt();
		
	}

	@After
	public void tearDown() throws Exception {
	}


	@Test
	public void test()
	{
		try {
			String testName = "Basic";
			ITable inputTable = xl.getTable("I-" + testName);

			Insight insight = new Insight();
			insight.setInsightId("1");
			
			System.out.println("Row Count >>" + inputTable.getRowCount());

			String value = (String)inputTable.getValue(0, "Pixel");
			List <String> pixels = new ArrayList<String>();
			
			String [] pixelParts = value.split(";"); 
			
			// execute everything but the last stage
			for(int partIndex = 0;partIndex < (pixelParts.length -1);partIndex++)
				pixels.add(pixelParts[partIndex] + ";");

			PixelRunner runner = insight.runPixel(pixels);
			
			pixels.clear();
			pixels.add(pixelParts[pixelParts.length - 1] + ";");

			runner = insight.runPixel(pixels);

			
			// this is where the comparison has to be
			System.out.println("Running Compare.. ");
			
			assertEquals(compareTask(runner, testName), true);
		} catch (DataSetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	// old code
	public void test2() {

		Insight insight = new Insight();
		insight.setInsightId("1");
		
		String pixel = "2+2;";
		
		List <String> pixels = new ArrayList<String>();
		pixels.add(pixel);
		pixels.add("CreateFrame(frameType=[GRID]);");
		pixels.add("(Database ( database = [ \"db394ac3-f9ee-460b-949e-ea9e96ecf4a8\" ] ) | Select ( Director , Title , Title__MovieBudget , Title__RevenueDomestic , Title__RevenueInternational , Title__RottenTomatoesAudience , Title__RottenTomatoesCritics , Nominated , Studio , Genre ) .as ( [ Director , Title , MovieBudget , RevenueDomestic , RevenueInternational , RottenTomatoesAudience , RottenTomatoesCritics , Nominated , Studio , Genre ] ) | Join ( ( Title , inner.join , Studio ) , ( Title , inner.join , Genre ) , ( Title , inner.join , Director ) , ( Title , inner.join , Nominated ) ) | Import ( ) );");
		pixels.add("Frame() | QueryAll()  | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 500 ) ;");
		
		PixelRunner runner = insight.runPixel(pixels);
		
		List <NounMetadata> results = runner.getResults();
		
		for(int resIndex = 0;resIndex < results.size();resIndex++)
			System.out.println("res....  " + results.get(resIndex));
		
		
		NounMetadata last = results.get(results.size()  - 1);
		
		// this is where the comparison has to be
		
		
		//assertEquals(compareTask(runner, true);
	}
	
	
	// make insight

	public boolean compareTask(PixelRunner runner, String testName)
	{
		boolean retValue = true;
		
		// 1. Get the output and see what is it trying to compare - row 1
		// 2. If it is data - try to get the complete json name - row 2
		// 3. If it is columns - try to get file name where that metadata is kept - row 2 
		// 4. Generate the JSON by using the pixel stream utility and passing the file
		// 5. Execute the appropriate 
		
		try {
			String sheetName = "O-" + testName;
			ITable table = xl.getTable(sheetName);

			// get the first row
			// pragma is the name of the column
			String pragma = (String)table.getValue(0, "pragma");
			
			String value = (String)table.getValue(1, "pragma");
			
			
			runScript("from deepdiff import DeepDiff");
			runScript("import json");
			runScript("from deepdiff import DeepDiff");
			
			// get the last piece out of the testFileName
			String [] fileParts = testFileName.split("/");
			String testFileDir = testFileName.replace("/" + fileParts[fileParts.length-1], "");

			String actFileName = testBaseFolder + "/" + testFileDir + "/" + Utility.getRandomString(10) + ".json";
			writePixelTask(runner, actFileName);
			
			File actualFile = new File(actFileName);
			
			String exclude = ", exclude_paths={\"root['pixelReturn'][0]['output']['data']\"}";
			
			if(table.getValue(0, "ExcludePattern") != null)
				exclude = ", exclude_paths = " + table.getValue(0, "ExcludePattern");
			
			if(pragma.equalsIgnoreCase("data") && table.getValue(0, "ExcludePattern") == null)
			{
				// do a set of routines here
				// take out the exclude
				exclude = "";
			}
			
			String act = Utility.getRandomString(10);
			String exp = Utility.getRandomString(10);
			
			// load actual file as json
			runScript("f" + act + " = open('" + actFileName + "')");
			runScript(act + " = json.load(f" + act + ", encoding='latin-1')");

			// load expected file as json
			runScript("e" + exp + " = open('" + value + "')");
			runScript(exp + " = json.load(e" + exp + ", encoding='latin-1')");
			
			// run the deep diff
			String pyVar = Utility.getRandomString(10);
			
			runScript(pyVar + "= DeepDiff(" + act + ", " + exp + exclude + ")");
			
			//repl();
			Object same = runScript("'values_changed' in " + pyVar);

			actualFile.delete();
				
			if(same instanceof Boolean)
				retValue = !(Boolean)same;
			else
				System.out.println("==" + same + "==");
			
			
			//repl();
			

			/*
			 * Overall Python script that does this. 
			// python
			// loads the data into python
			
			// from deepdiff import DeepDiff
			// f = open('c:/temp/output.json')
			// data = json.load(f);
			
			// delta = DeepDiff(data, data, ignore_order=True) - gets you the delta between the 2 files
			// (DeepDiff(data, data, exclude_paths={"root['ingredients']"}))
			//DeepDiff(data, data2, exclude_paths={"root['pixelReturn'][4]['output']['data']"})
			
			// main test directory - we can change the location later
			// need to compare this row by row column by column or need to ask george is there is a way to do this easily
			 * 
			 * 
			 * len(json['pattern']) will give you json length
			 * 
			 * 
			 */
		} catch (DataSetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return retValue;
	}
	
	public void repl()
	{
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String data = "2+2";
			while(!(data = br.readLine()).equalsIgnoreCase("q"))
			{
				Object ret = runScript(data);
				System.out.println("Output >> " + ret);
				System.out.println("Command (q to quit) !! ");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	
	public void writeTaskData(BasicIteratorTask task, File file)
	{
		try {
			boolean first = true;
			// need to compare this row by row column by column or need to ask george is there is a way to do this easily
			PrintWriter pw = new PrintWriter(new FileWriter(file));
			while(task.hasNext())
			{
				IHeadersDataRow row = task.next();
				if(first)
				{
					String [] headers = row.getHeaders();
					pw.write(getRow(headers) +"\n");
					first = false;
				}			
				Object [] values = row.getValues();
				pw.write(getRow(values) +"\n");
			}
			pw.flush();
			pw.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public String getRow(Object [] row)
	{
		StringBuffer buf = new StringBuffer();
		for(int colIndex = 0;colIndex < row.length;colIndex++)
		{
			if(colIndex != 0)
				buf.append(",");
			buf.append(row[colIndex]);
		}
		return buf+"";
	}
	
	
	
	
	public void writePixelTask(PixelRunner runner, String fileName)
	{
		//PrintStream ps = new PrintStream(new FileOutputStream("c:/temp/output.json"));
		PixelStreamUtility.writePixelData(runner, new File(fileName), false);
	}
	
	public Object runScript(String... script) {

		py.command = script;
		// get the monitor
		Object monitor = py.getMonitor();
		Object response = null;
		synchronized(monitor) {
			try {
				// tell py thread to run
				monitor.notify();
				// wait 4 seconds if not proceed
				monitor.wait(4000);
			} catch (Exception ignored) {

			}
			if(script.length == 1) {
				response = py.response.get(script[0]);
			} else {
				response = py.response;
			}
		}

		return response;
	}

}
