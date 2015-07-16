package prerna.test;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.junit.*;

import prerna.algorithm.learning.weka.WekaAprioriAlgorithm;
import prerna.ds.BTreeDataFrame;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.util.DIHelper;
import prerna.util.Utility;
import weka.associations.Item;



/**
* WekaAprioriAlgorithmTest checks the 3 main methods of the WekaAprioriAlgorithm class
* 1)Checking if the WekaAprioriAlgorithm works against Movie_DB 
* 2)Generate Decision Rule Visualization.
* 3)Generate Decision Rule Table
*
* @author  Betrand Tamunang
* @version 1.0
* @since   06-08-2015 
* Questions? Email btamunang@deloitte.com
*/
public class WekaAprioriAlgorithTest {

	private static String workingDir = System.getProperty("user.dir");
	
	private WekaAprioriAlgorithm wekaAprioriAlgorithm;
	
	private static BendersTools bTools;
	private static BTreeDataFrame bTree;
	
	static int testCounter;
	
	@BeforeClass
	public static void setupOnce(){
		bTools = new BendersTools();
		System.out.println("Test Started..");
	}
	
	@Before
	public void setUp() throws Exception{
		testCounter++;
		System.out.println("Test " + testCounter + "...");
		
		
		//Set the Sudo-Prop
		System.setProperty("file.separator", "/");
		String workingDir = System.getProperty("user.dir");
		String propFile = workingDir + "/RDF_Map.prop";
		DIHelper.getInstance().loadCoreProp(propFile);
		PropertyConfigurator.configure(workingDir + "/log4j.prop");
	}

	@After
	public void tearDown() throws Exception {
		System.out.println("Tear Down ...");
	}

	/**execute Unit Test
	 * 
	 * 1) Open Engine and run query for WekaAprioriAlgorithm
	 * 2) Get Actual Results
	 * 3) Compare actual results data with pseudo data. 
	 * 
	 * @param Movie_DB, setPremises(), setConsequences(), setCounts(), execute()
	 * 
	 * @throws Exception
	 */
	@Ignore
	@Test
	public void Test_execute() throws Exception{
		String propFile = workingDir + "//db//" + "Movie_DB.smss";
		
		//Open Engine and Run Execute
		BigDataEngine engine = new BigDataEngine();
		Properties prop = new Properties();
		FileInputStream fileIn = null;
		try {
			fileIn = new FileInputStream(propFile);
			prop.load(fileIn);			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(fileIn != null) {
					fileIn.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		engine = (BigDataEngine) Utility.loadEngine(propFile, prop);
		
		ArrayList<Object[]> list = null;
		String [] names = null;

		//Preprepared Query for this test
		String query = "SELECT DISTINCT ?Title ?Genre ?Director ?Nominated ?Studio ?DomesticRevenue ?InternationalRevenue ?Budget ?RottenTomatoesCritics ?RottenTomatoesAudience "
				+ "WHERE {{?Title <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Title>}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/Revenue-Domestic> ?DomesticRevenue }"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/Revenue-International> ?InternationalRevenue}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/MovieBudget> ?Budget}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Critics> ?RottenTomatoesCritics }"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Audience> ?RottenTomatoesAudience }"
				+ "{?Director <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Director>}"
				+ "{?Genre <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Genre>}"
				+ "{?Nominated <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Nominated>}"
				+ "{?Studio <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Studio>}"
				+ "{?Title <http://semoss.org/ontologies/Relation/DirectedBy> ?Director}"
				+ "{?Title <http://semoss.org/ontologies/Relation/BelongsTo> ?Genre}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Was> ?Nominated}"
				+ "{?Title <http://semoss.org/ontologies/Relation/DirectedAt> ?Studio}} ORDER BY ?Title  LIMIT 2";

		list = new ArrayList<Object[]>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		names = sjsw.getVariables();
		int length = names.length;
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			Object[] row = new Object[length];
			int i = 0;
			for(; i < length; i++) {
				row[i] = sjss.getVar(names[i]);
			}
			list.add(row);
		}	

		//Test the method
		bTree = bTools.createBTree(names, list);
		wekaAprioriAlgorithm = new WekaAprioriAlgorithm();
		wekaAprioriAlgorithm.runAlgorithm(bTree);
		
		
		//Get execute Results
		Map<Integer, Collection<Item>> premisesResult = null; //wekaAprioriAlgorithm.getPremises(); 
		Map<Integer, Collection<Item>> consequencesResult = null; //wekaAprioriAlgorithm.getConsequences();
		Map<Integer, Integer> countsResult = null; //wekaAprioriAlgorithm.getCounts();
		Map<Integer, Double> confidenceIntervalsResult = null; //wekaAprioriAlgorithm.getConfidenceIntervals();
		
		//Assertions 
		//Scenario#1
		Boolean tester = false;
		int n = 3;
		if(premisesResult.get(n).toArray()[0].toString().contains("Director=Danny_Boyle")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Title=127_Hours")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run execute assertion Scenario# 1");
		
		//Scenario#2
		n = 6;
		if(premisesResult.get(n).toArray()[0].toString().contains("Studio=Fox")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Title=127_Hours")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run execute assertion Scenario# 2");
		
		//Scenario#3
		n = 8;
		if(premisesResult.get(n).toArray()[0].toString().contains("DomesticRevenue=1.833523E7")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Title=127_Hours")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run execute assertion Scenario# 3");
		
		//Scenario#4
		n = 10;
		if(premisesResult.get(n).toArray()[0].toString().contains("InternationalRevenue=0.0")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Title=127_Hours")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
				tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run execute assertion Scenario# 4");
		
		//Scenario#5
		n = 12;
		if(premisesResult.get(n).toArray()[0].toString().contains("Budget=1.8E7")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Title=127_Hours")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
				tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run execute assertion Scenario# 5");

		//Scenario#6
		n = 39;
		if(premisesResult.get(n).toArray()[0].toString().contains("DomesticRevenue=4.8352E7")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Genre=Drama")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run execute assertion Scenario# 6");
		
		//Scenario#7
		n = 60;
		if(premisesResult.get(n).toArray()[0].toString().contains("Director=Steve_McQueen")
				&& consequencesResult.get(n).toArray()[0].toString().contains("RottenTomatoesAudience=0.92")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run execute assertion Scenario# 7");
		
		//Scenario#8
		n = 75;
		if(premisesResult.get(n).toArray()[0].toString().contains("Studio=Fox")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Nominated=Y")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
				tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run execute assertion Scenario# 8");
		
		
		//Scenario#9
		n = 87;
		if(premisesResult.get(n).toArray()[0].toString().contains("Studio=Fox_Searchlight")
				&& consequencesResult.get(n).toArray()[0].toString().contains("DomesticRevenue=4.8352E7")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
				tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run execute assertion Scenario# 9");
		
		//Scenario#10
		n = 96;
		if(premisesResult.get(n).toArray()[0].toString().contains("DomesticRevenue=1.833523E7")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Studio=Fox")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run execute assertion Scenario# 10");
		
		engine.commitOWL();
		engine.commit();
		engine.closeDB();
		System.out.println("Test " + testCounter + " Complete");
	}
	
	/**generateDecisionRuleVizualization Unit Test
	 * 
	 * 1) Get input data from execute()
	 * 2) Get Actual generateDecisionRuleVizualization Results
	 * 3) Compare actual results data with pseudo data. 
	 * 
	 * @param Movie_DB, setPremises(), setConsequences(), setCounts(), execute()
	 * 
	 * @throws Exception
	 */
	@Ignore
	@Test
	public void Test_generateDecisionRuleVizualization() throws Exception {
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		String [] names;
		String propFile = workingDir + "//db//" + "Movie_DB.smss";
		
		//Run Execute
		BigDataEngine engine = new BigDataEngine();
		Properties prop = new Properties();
		FileInputStream fileIn = null;
		try {
			fileIn = new FileInputStream(propFile);
			prop.load(fileIn);			
		} catch (IOException e) {
			e.printStackTrace();
			
		} finally {
			try {
				if(fileIn != null) {
					fileIn.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		engine = (BigDataEngine) Utility.loadEngine(propFile, prop);

		//Preprepared Query for this test
		String query = "SELECT DISTINCT ?Title ?Genre ?Director ?Nominated ?Studio ?DomesticRevenue ?InternationalRevenue ?Budget ?RottenTomatoesCritics ?RottenTomatoesAudience "
				+ "WHERE {{?Title <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Title>}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/Revenue-Domestic> ?DomesticRevenue }"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/Revenue-International> ?InternationalRevenue}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/MovieBudget> ?Budget}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Critics> ?RottenTomatoesCritics }"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Audience> ?RottenTomatoesAudience }"
				+ "{?Director <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Director>}"
				+ "{?Genre <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Genre>}"
				+ "{?Nominated <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Nominated>}"
				+ "{?Studio <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Studio>}"
				+ "{?Title <http://semoss.org/ontologies/Relation/DirectedBy> ?Director}"
				+ "{?Title <http://semoss.org/ontologies/Relation/BelongsTo> ?Genre}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Was> ?Nominated}"
				+ "{?Title <http://semoss.org/ontologies/Relation/DirectedAt> ?Studio}} ORDER BY ?Title  LIMIT 2";

		list = new ArrayList<Object[]>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		names = sjsw.getVariables();
		int length = names.length;
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			Object[] row = new Object[length];
			int i = 0;
			for(; i < length; i++) {
				row[i] = sjss.getVar(names[i]);
			}
			list.add(row);
		}	
		
		bTree = bTools.createBTree(names, list);
		wekaAprioriAlgorithm = new WekaAprioriAlgorithm();
		wekaAprioriAlgorithm.runAlgorithm(bTree);
		
		//Get Actual generateDecisionRuleVizualization Results
		Hashtable<String, Object> hashActual = wekaAprioriAlgorithm.generateDecisionRuleVizualization();
		String[] headers =  (String[]) hashActual.get("headers");
		
		//Assertions
		//Scenario# 1
		Boolean tester = false;
		if(headers[0].contains("Count") && hashActual.get("data").toString().contains("2, 1.00, Nominated = Y, Genre = Drama")){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run GenerateDecisionRuleVizualization Test Scenario# 1");
		
		//Scenario# 2
		if(headers[1].contains("Confidence") && hashActual.get("data").toString().contains("2, 1.00, Genre = Drama, Nominated = Y")){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run GenerateDecisionRuleVizualization Test Scenario# 2");
		
		//Scenario# 3
		if(headers[2].contains("Premises") && hashActual.get("data").toString().contains("1, 1.00, Title = 127_Hours, Genre = Drama")){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run GenerateDecisionRuleVizualization Test Scenario# 3");
		
		//Scenario# 4
		if(headers[3].contains("Consequence") && hashActual.get("data").toString().contains("1, 1.00, Director = Danny_Boyle, Title = 127_Hours")){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		System.err.println("Successfully run GenerateDecisionRuleVizualization Test Scenario# 4");
		
		//Close Engine
		engine.commitOWL();
		engine.commit();
		engine.closeDB();
		System.out.println("Test " + testCounter + " Complete");
	}
	
	
	/**generateDecisionRuleTable Unit Test
	 * 
	 * 1) Run query using the execute() to get input data.
	 * 2) Get Actual generateDecisionRuleTable Results by calling generateDecisionRuleTable()
	 * 3) Compare actual results data with pseudo data.
	 * 
	 * @param Movie_DB, setPremises(), setConsequences(), setCounts(), execute()
	 * 
	 * @throws Exception
	 */
	@Ignore
	@Test
	public void Test_generateDecisionRuleTable() throws Exception{
		// PART 1
		ArrayList<Object[]> list = new ArrayList<Object[]>();
		String[] names = new String[0];
		String propFile = workingDir + "//db//" + "Movie_DB.smss";
		
		//Run Execute
		BigDataEngine engine = new BigDataEngine();
		Properties prop = new Properties();
		FileInputStream fileIn = null;
		try {
			fileIn = new FileInputStream(propFile);
			prop.load(fileIn);			
		} catch (IOException e) {
			e.printStackTrace();
			
		} finally {
			try {
				if(fileIn != null) {
					fileIn.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		engine = (BigDataEngine) Utility.loadEngine(propFile, prop);

		//Preprepared Query for this test
		String query = "SELECT DISTINCT ?Title ?Genre ?Director ?Nominated ?Studio ?DomesticRevenue ?InternationalRevenue ?Budget ?RottenTomatoesCritics ?RottenTomatoesAudience "
				+ "WHERE {{?Title <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Title>}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/Revenue-Domestic> ?DomesticRevenue }"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/Revenue-International> ?InternationalRevenue}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/MovieBudget> ?Budget}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Critics> ?RottenTomatoesCritics }"
				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Audience> ?RottenTomatoesAudience }"
				+ "{?Director <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Director>}"
				+ "{?Genre <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Genre>}"
				+ "{?Nominated <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Nominated>}"
				+ "{?Studio <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Studio>}"
				+ "{?Title <http://semoss.org/ontologies/Relation/DirectedBy> ?Director}"
				+ "{?Title <http://semoss.org/ontologies/Relation/BelongsTo> ?Genre}"
				+ "{?Title <http://semoss.org/ontologies/Relation/Was> ?Nominated}"
				+ "{?Title <http://semoss.org/ontologies/Relation/DirectedAt> ?Studio}} ORDER BY ?Title ";


		list = new ArrayList<Object[]>();
		ISelectWrapper sjsw = Utility.processQuery(engine, query);
		names = sjsw.getVariables();
		int length = names.length;
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			Object[] row = new Object[length];
			for(int i = 0; i < length; i++) {
				row[i] = sjss.getVar(names[i]);
			}
			list.add(row);
		}	

		wekaAprioriAlgorithm = new WekaAprioriAlgorithm();
		bTree = bTools.createBTree(names, list);
		wekaAprioriAlgorithm.runAlgorithm(bTree);

		wekaAprioriAlgorithm.generateDecisionRuleTable();
		
		ArrayList<Object[]> retListResult = null; //wekaAprioriAlgorithm.getRetList();
		String[] retNamesResult = null; //wekaAprioriAlgorithm.getRetNames();
							
		//Assertions
		//Scenario# 1
		Boolean testResult = false;
		for(int i = 0; i < retListResult.size(); i++ ){
		if(retNamesResult[0].contains("Genre") 
				&& retListResult.get(i)[9].toString().contains("99")
				&& retListResult.get(i)[10].toString().contains("1.00")){
			testResult = true;
			}
		}
		assertTrue("Information is incorrect: ", testResult);
		testResult = false;
		System.err.println("Successfully run GenerateDecisionRuleTable Scenario# 1");
		
		//Scenario# 2
		for(int i = 0; i < retListResult.size(); i++ ){
		if(retNamesResult[3].contains("Studio") &&
				retListResult.get(i)[9].toString().contains("52") &&
				retListResult.get(i)[10].toString().contains("0.96")){
			testResult = true;
			}
		}
		assertTrue("Information is incorrect: ", testResult);
		testResult = false;
		System.err.println("Successfully run GenerateDecisionRuleTable Scenario# 2");
		
		//Scenario# 3
		for(int i = 0; i < retListResult.size(); i++ ){
			if(retNamesResult[9].contains("Count") && 
					retListResult.get(i)[9].toString().contains("51") &&
					retListResult.get(i)[10].toString().contains("0.96")
					){
				testResult = true;
				}
			}
		assertTrue("Information is incorrect: ", testResult);
		testResult = false;
		System.err.println("Successfully run GenerateDecisionRuleTable Scenario# 3");
			
		engine.commitOWL();
		engine.commit();
		engine.closeDB();
		System.out.println("Test " + testCounter + " Complete");
	}
	
}
