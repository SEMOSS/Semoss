package prerna.test;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import prerna.algorithm.learning.weka.WekaAprioriAlgorithm;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import weka.associations.Item;

/**
 * WekaAprioriAlgorithmtTest checks the execute method of the WekaAprioriAlgorithm Class
 * 
 * 1) Testing execute()
 *
 * @author  Betrand T.
 * @version 1.0
 * @since   05-19-2015
 * Questions? Email btamunang@deloitte.com
 */
public class WekaAprioriAlgorithTest {

	private static String workingDir = System.getProperty("user.dir");
	private BigDataEngine engine;
	//Directories
	private static String semossDirectory;
	private static String dbDirectory;
	private static ImportDataProcessor processor;
	private ISelectWrapper wrapper;

	private WekaAprioriAlgorithm wekaAprioriAlgorithm;
	
	static int testCounter;
	
	@BeforeClass
	public static void setupOnce(){
		System.out.println("Test Started..");
		semossDirectory = System.getProperty("user.dir");
		semossDirectory = semossDirectory.replace("\\", "\\\\");
		dbDirectory = semossDirectory + "\\db\\";


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
		System.out.println("Test Completed..");
	}

	//Execute
	@Test
	public void Test_execute() throws Exception{
		String propFile = workingDir + "//db//" + "Movie_DB.smss";
		
		//Run Execute
		//BigDataEngine engine = loadEngine(propFile);
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
		wekaAprioriAlgorithm = new WekaAprioriAlgorithm(list, names);
		wekaAprioriAlgorithm.execute(); //This is the Method Being tested
		
		
		//Get execute Results
		Map<Integer, Collection<Item>> premisesResult = wekaAprioriAlgorithm.getPremises(); 
		Map<Integer, Collection<Item>> consequencesResult = wekaAprioriAlgorithm.getConsequences();
		Map<Integer, Integer> countsResult = wekaAprioriAlgorithm.getCounts();
		Map<Integer, Double> confidenceIntervalsResult = wekaAprioriAlgorithm.getConfidenceIntervals();
		
		 //Asserts 
		
		//Case#1
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
		
		//Case#2
		n = 6;
		if(premisesResult.get(n).toArray()[0].toString().contains("Studio=Fox")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Title=127_Hours")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		
		//Case#3
		n = 8;
		if(premisesResult.get(n).toArray()[0].toString().contains("DomesticRevenue=1.833523E7")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Title=127_Hours")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		
		//Case#4
		n = 10;
		if(premisesResult.get(n).toArray()[0].toString().contains("InternationalRevenue=0.0")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Title=127_Hours")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
				tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		
		//Case#5
		n = 12;
		if(premisesResult.get(n).toArray()[0].toString().contains("Budget=1.8E7")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Title=127_Hours")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
				tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		

		//Case#6
		n = 39;
		if(premisesResult.get(n).toArray()[0].toString().contains("DomesticRevenue=4.8352E7")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Genre=Drama")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		
		//Case#7
		n = 60;
		if(premisesResult.get(n).toArray()[0].toString().contains("Director=Steve_McQueen")
				&& consequencesResult.get(n).toArray()[0].toString().contains("RottenTomatoesAudience=0.92")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		
		//Case#8
		n = 75;
		if(premisesResult.get(n).toArray()[0].toString().contains("Studio=Fox")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Nominated=Y")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
				tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		
		
		//Case#9
		n = 87;
		if(premisesResult.get(n).toArray()[0].toString().contains("Studio=Fox_Searchlight")
				&& consequencesResult.get(n).toArray()[0].toString().contains("DomesticRevenue=4.8352E7")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
				tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		
		//Case#10
		n = 96;
		if(premisesResult.get(n).toArray()[0].toString().contains("DomesticRevenue=1.833523E7")
				&& consequencesResult.get(n).toArray()[0].toString().contains("Studio=Fox")
				&& countsResult.get(n) == 1
				&& confidenceIntervalsResult.get(n) == 1.0){
			tester = true;
		}
		assertTrue("Information is incorrect: ", tester);
		tester = false;
		
		engine.commit();
		engine.closeDB();
		System.out.println("Test " + testCounter + " complete");
	}




	private BigDataEngine loadEngine(String engineLocation){
		BigDataEngine engine = new BigDataEngine();
		FileInputStream fileIn;
		Properties prop = new Properties();
		try {
			fileIn = new FileInputStream(engineLocation);
			prop.load(fileIn);
			//SEP
			try {
				String engines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";

				String engineName = prop.getProperty(Constants.ENGINE);
				String engineClass = prop.getProperty(Constants.ENGINE_TYPE);
				//TEMPORARY
				// TODO: remove this
				if(engineClass.equals("prerna.rdf.engine.impl.RDBMSNativeEngine")){
					engineClass = "prerna.engine.impl.rdbms.RDBMSNativeEngine";
				}
				else if(engineClass.startsWith("prerna.rdf.engine.impl.")){
					engineClass = engineClass.replace("prerna.rdf.engine.impl.", "prerna.engine.impl.rdf.");
				}
				engine = (BigDataEngine)Class.forName(engineClass).newInstance();
				engine.setEngineName(engineName);
				if(prop.getProperty("MAP") != null) {
					engine.addProperty("MAP", prop.getProperty("MAP"));
				}
				engine.openDB(engineLocation);
				engine.setDreamer(prop.getProperty(Constants.DREAMER));
				engine.setOntology(prop.getProperty(Constants.ONTOLOGY));

				// set the core prop
				if(prop.containsKey(Constants.DREAMER))
					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.DREAMER, prop.getProperty(Constants.DREAMER));
				if(prop.containsKey(Constants.ONTOLOGY))
					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.ONTOLOGY, prop.getProperty(Constants.ONTOLOGY));
				if(prop.containsKey(Constants.OWL)) {
					DIHelper.getInstance().getCoreProp().setProperty(engineName + "_" + Constants.OWL, prop.getProperty(Constants.OWL));
					engine.setOWL(prop.getProperty(Constants.OWL));
				}

				// set the engine finally
				engines = engines + ";" + engineName;
				DIHelper.getInstance().setLocalProperty(engineName, engine);
				DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			//SEP
			fileIn.close();
			prop.clear();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return engine;
	}
	
}
