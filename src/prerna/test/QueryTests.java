package prerna.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Vector;

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.om.Insight;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
* QueryTest has one test within it concerning queries:
* 1) Recursively check the query of every insight, in every perspective, 
* 	 in every DB against the respective journal file 
* 	 via the smss file in the db folder
* 
*
* @author  August Bender
* @version 1.1
* @since   03-20-2015
* Questions? Email abender@deloitte.com
*/
public class QueryTests {

	@SuppressWarnings({ "unused" })
	private BigDataEngine engine;
	//Directories
	private static String semossDirectory;
	private static String dbDirectory;

	private static String serverDirectory = "test\\test_stagging\\db\\";
	
	@BeforeClass 
	public static void setUpOnce(){
		semossDirectory = System.getProperty("user.dir");
		semossDirectory = semossDirectory.replace("\\", "\\\\");
		dbDirectory = semossDirectory + "\\db\\";
		//dbDirectory = serverDirectory;
		
	}

	@Before
	public void setUp() throws Exception {
		engine = new BigDataEngine();
		
		// Set the Prop (Created in Prerna.ui.main.Starter.java)
		System.setProperty("file.separator", "/");
		String workingDir = System.getProperty("user.dir");
		String propFile = workingDir + "/RDF_Map.prop";
		DIHelper.getInstance().loadCoreProp(propFile);
		PropertyConfigurator.configure(workingDir + "/log4j.prop");
		System.out.println("Test Started..");
	}
	
	@After
	public void tearDown() throws Exception {
		System.out.println("Test Ended..");
	}
	
	
	/**
	 * testAllQueries goes through the DB folder, identifies all active DBs via their .smss files.
	 * The test checks every perspective and insight there in against the proper engine type.
	 * 
	 * @asserts the Query is not Null
	 * @asserts the Query returns a value
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testAllQueries() throws  IOException{
		//Get all DBs {Loop}
		//Get all Perspectives for each DB {Loop}
		//Get all insight questions from each DB {Loop}
		//Check the query {Method}
		
		//Get all DBs by their .smss
		File dbFolder = new File(dbDirectory);
		String[] listOfFiles = dbFolder.list();
		
		//# of .smss and put them in a String[]
		int numberOfSmss = 0;
		for (int i = 0; i < listOfFiles.length; i++){
			if(listOfFiles[i].contains(".smss")){
			numberOfSmss++;
			}
		}
		String[] listOfDB = new String[numberOfSmss];
		int a = 0;
		for (int i = 0; i < listOfFiles.length; i++){
			if(listOfFiles[i].contains(".smss")){
				if(listOfFiles[i] != "LocalMasterDatabase.smss"){
					//TO DO: change this to any hidden databases
					listOfDB[a] = listOfFiles[i];
					System.out.println("list of Files: "+listOfDB[a]);
					a++;
				}
			}
		}
		
		
		/*Get Perspectives from each DB
		*There are three layers of for-loops:
		*1: Engines
		*2: That Engines Perspective
		*3: That Perspectives Insight(Question Query)
		*/
		
		//Prep
		BigDataEngine engine = new BigDataEngine();
		StopWatch timer = new StopWatch();
		
		int queryCount= 0;
		int checkedCount = 0;
		int skippedCount = 0;
		int customCount = 0;
		int dualEngineCounter = 0;
		int malformedCount = 0;
		String malformedArray = "";
		timer.start();
		
		//Engine
		for (int i = 0; i < listOfDB.length; i++){
			String engineLocation = dbDirectory + listOfDB[i];
			/*DeBug*/System.out.println("break engine: "+engineLocation);
			engine = loadEngine(engineLocation);
			//Perspectives
			Vector<String> perspec = engine.getPerspectives();
			/*DeBug*/System.out.println("break Perspective: "+perspec.get(0));
			if(perspec != null){
				for(int k = 0; k < perspec.size(); k++){
					String currentPerspec = perspec.get(k);
					//Insights
					Vector<String> insights = engine.getInsights(currentPerspec, engine.getEngineName());
					/*DeBug*/System.out.println("break insight");
					if(insights != null){
						for(int l = 0; l < insights.size(); l++){
							String currentInsight = insights.get(l);
							Insight test = engine.getInsight(currentInsight);
							//Query Based Check
							String query = test.getSparql();
							String queryHome = "Engine: "+engine.getEngineName()+", Perspective: " + currentPerspec + ", Insight: "+currentInsight;
							System.out.println(queryHome);
							
							//Prep the Query for engine use
							query = prepQuery(query);
							
							//Final Checks and counts
							//Broken Query Pool; TO-DO: FIX all in Broken Query Pool
							/*if(
									query.contains("555666")){
								query = "NULL";
								skippedCount++;
								for(int m = 0; m <5; m++){
									System.out.println("***** ***** *****");
								}
							}
							////Detect Custom play-sheets with abstract Query
							else*/ if(query.contains("{") == false) {
								customCount++;
							}
							//Detect Dual Engines
							else if (query.contains("&") == true && query.contains("&&") == false){
								dualEngineCounter++;
							}
							//Detect Custom play-sheets with no Query
							else if(query.contains("NULL") == true || query.contains("Null") == true){
								customCount++;
							} 
							else {
								try {
									assertTrue(queryHome +": is broken...",checkQuery(engine, query));
								} catch (MalformedQueryException e) {
									System.out.println("MALFORMED");
									malformedArray += queryHome + "\r\n";
									malformedCount++;
									e.printStackTrace();
								} catch (NullPointerException e) {
									System.out.println("MALFORMED");
									malformedArray += queryHome + "\r\n" +"		"+ query + "\r\n" + "\r\n";
									malformedCount++;
									e.printStackTrace();
								}
								checkedCount++;
							}
							queryCount++;
							System.out.println("COUNT: "+queryCount);
						}
					}
				}
			}
			engine.closeDB();
		}
		System.out.println("~~~~~~~~~~~ DBs Checked ~~~~~~~~~~");
		for(int s = 0; s < listOfDB.length; s++){
			System.out.println(s+1+": "+listOfDB[s]);
		}
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		System.out.println("Total Test Time Elapsed:     "+timer.getElapsedTime());
		System.out.println("Total Number of Insights:    "+queryCount);
		System.out.println("Number of Insights Checked:  "+checkedCount);
		System.out.println("Number of Insights, Custom:  "+customCount);
		System.out.println("Number of Dual Engines:      "+dualEngineCounter);
		System.out.println("Number of Insights Ignored:  "+skippedCount);
		System.out.println("Malformed Queries:           "+malformedCount);
		System.out.println("Accuracy of Insight Checker: "+((100*(checkedCount+customCount+dualEngineCounter))/(queryCount))+"%");
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
		if(malformedArray != ""){System.out.println("Malformations Caught: "+"\r\n"+malformedArray);}
		else{System.out.println("Your Semoss is a Happy Semoss!");}
	}
	
	/**
	 * perpQuery sets all the user input requirements for the query to run and removes
	 * any information attached to the query that would prevent it from being run against the engine.
	 * 
	 * @param Query to be prepped
	 * 
	 * @return Perpped Query
	 */
	private String prepQuery(String query){
		//Clustering; Remove all clustering info so query can be processed
		String[] clusterRemoval= {"+++@NumberOfClusters-OverrideCluster@","+++J48","+++PART", "+++DecisionTable", "+++DecisionStump"
								,"+++REPTree", "+++LMT", "+++SimpleLogistic", "+++@KNeighbors-K@"}; //Add new Cluster param types here
		if(query.contains("+++")){
			for(int i = 0; i < clusterRemoval.length; i++){
				if(query.contains(clusterRemoval[i])){
					query = query.replace(clusterRemoval[i], "");
					System.out.println("Part Removed: "+clusterRemoval[i]);
				}
			}
		}
		
		//Dates; removed them, they are dealt with in the play-sheet and not the engine
		String[] dateRemoval = {"@Year-OverrideYear@;", "@Month-OverrideMonth@;"};//Add additional time and date params here
		if(query.contains("CONSTRUCT") && query.contains("-Override")){
			for(int i = 0; i < dateRemoval.length; i++){
				if(query.contains(dateRemoval[i])){
					query = query.replace(dateRemoval[i], "");
					System.out.println("Part Removed: "+dateRemoval[i]);
				}
			}
		}
		
		//Params; find and insert query params normally put in by the user
		if(query.contains("BIND") || query.contains("DISTINCT")){
			Hashtable paramHash = Utility.getParams(query);
			Utility.fillParam(query, paramHash);
			
		}
		return query;
	}
	
	/**
	 * checkQuery is a short method that determines the query type and which method should be called
	 * from the engine to process the query against the jnl file
	 * 
	 * @param BigDataEngine that Query Exists in
	 * @param Query to be Checked
	 * 
	 * @return Boolean - True if Query works
	 */
	private boolean checkQuery(BigDataEngine engine, String query) throws MalformedQueryException, NullPointerException{
		//Limit the results
		if(query.contains("BINDINGS")){
			query = query.replace("BINDINGS", "LIMIT 3 BINDINGS");
		}
		if(query.contains("LIMIT") || query.contains("limit")){} 
		else {query += " LIMIT 3";}
		
		//get Check
		Boolean result = false;
		System.out.println("Query: "+query);
		
		if(query.contains("CONSTRUCT")){
			GraphQueryResult res  = engine.execGraphQuery(query);
			if(res.toString() != ""){result = true;}
			try {
				res.close();	
			} catch( QueryEvaluationException e) {
				e.printStackTrace();
				System.out.println("Data remains in Cashe...");
			}
		} 
		else if(query.contains("ASK")) {
			result = engine.execAskQuery(query);
		} 
		else if (query.contains("SELECT")){
			TupleQueryResult res = engine.execSelectQuery(query);
			if(res.toString() != ""){result = true;}
			try {
				res.close();	
			} catch( QueryEvaluationException e) {
				e.printStackTrace();
				System.out.println("Data remains in Cashe...");
			}
		}
		System.out.println("");
		return result;
	}
	
	/**
	 * Opens the Engine via it's .smss file and sets up DIHelper through
	 * the Utility file and a prop
	 * 
	 * @param EngineLocation file path of DB's .smss file
	 * 
	 * @return BigDataEngine that has been successfully loaded
	 */
	private BigDataEngine loadEngine(String engineLocation){
		BigDataEngine engine = new BigDataEngine();
		FileInputStream fileIn = null;
		Properties prop = new Properties();
		try {
			fileIn = new FileInputStream(engineLocation);
			prop.load(fileIn);
			engine = (BigDataEngine) Utility.loadEngine(engineLocation, prop);
			fileIn.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return engine;
	}
	
}
