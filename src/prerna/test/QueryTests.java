package prerna.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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

import prerna.engine.impl.rdf.BigDataEngine;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class QueryTests {

	//Directories
		private static String semossDirectory;
		private static String dbDirectory;
		
		@BeforeClass 
		public static void setUpOnce(){
			semossDirectory = System.getProperty("user.dir");
			semossDirectory = semossDirectory.replace("\\", "\\\\");
			dbDirectory = semossDirectory + "/db/";
			//dbDirectory = serverDirectory;
			
		}

		@Before
		public void setUp() throws Exception {
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
		@Test
		public void testAllQueries() throws  IOException{
			//Get all DBs {Loop}
			//Get the DB's Engine
				//Get all Perspectives for each DB {Loop}
					//Get all insight questions from each DB {Loop}
			//Check the query {Method}
			
			//Get all DBs by their .smss
			File dbFolder = new File(dbDirectory);
			String[] listOfFiles = dbFolder.list();
			
			//# of .smss and put them in a String[]
			int numberOfSmss = 0;
			for (int i = 0; i < listOfFiles.length; i++){
				if(listOfFiles[i].contains(".smss") && !listOfFiles[i].contains("LocalMasterDatabase")){
				numberOfSmss++;
				}
			}
			String[] listOfDB = new String[numberOfSmss];
			int a = 0;
			for (int i = 0; i < listOfFiles.length; i++){
				if(listOfFiles[i].contains(".smss") && !listOfFiles[i].contains("LocalMasterDatabase")){
						//TO DO: change this to any hidden databases
						listOfDB[a] = listOfFiles[i];
						System.out.println("list of Files: "+listOfDB[a]);
						a++;
				}
			}
			
			
			/*Get Perspectives from each DB
			*There are three layers of for-loops:
			*1: Engines
			*2: That Engines Perspective
			*3: That Perspectives Insight(Question Query)
			*/
			//Prep
			BigDataEngine engine;
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
			for (int i = 0; i < listOfDB.length; i++) {
				String engineLocation = dbDirectory + listOfDB[i];
				if (!engineLocation.contains("null")) {
					engine = megaLoader(engineLocation);
					// Perspectives
					Vector<String> perspec = new Vector<String>();
					try {
						perspec = engine.getPerspectives();
					} catch (NullPointerException e) {
						e.printStackTrace();
					}
					for (int k = 0; k < perspec.size(); k++) {
						String currentPerspec = perspec.get(k);
						// Insights
						Vector<String> insights = new Vector<String>();
						try {
							insights = engine.getInsights(currentPerspec, "");
						} catch (NullPointerException e) {
							e.printStackTrace();
						}
						//Queries
						for (int l = 0; l < insights.size(); l++) {
							String query = engine.getInsight(insights.get(l)).getSparql();
							
							//Check for Custom
							if (!query.contains("{")) {
								customCount++;
							}
							// Detect Dual Engines
							else if (query.contains("&") && !query.contains("&&")) {
								dualEngineCounter++;
							}
							// Detect Custom play-sheets
							else if (  query.contains("NULL")
									|| query.contains("Null")
									|| query.contains("$LPI")
									|| query.contains("$LPNI")
									|| query.contains("HR_Core$")
									|| query.contains("$HPI")
									|| query.contains("$HPNI")
							// || query.contains("") == true
							) {
								customCount++;
							} else {
								
								String queryHome = ("Engine: " + engine.getEngineName()
										+ ", Perspective: " + currentPerspec
										+ ", Insight: " + insights.get(l));
								System.out.println(queryHome);
								try {
									query = prepQuery(query); //Prep the Query for engine use
									assertTrue(queryHome +": is broken...", checkQuery(engine, query));
									checkedCount++;
								} catch (MalformedQueryException e) {
									System.out.println("MALFORMED");
									malformedArray += queryHome + "\r\n";
									malformedCount++;
									e.printStackTrace();
								} catch (NullPointerException e) {
									System.out.println("NULL");
									malformedArray += queryHome + "\r\n" +"		"+ query + "\r\n" + "\r\n";
									malformedCount++;
									e.printStackTrace();
								}
								
								System.out.println("QUERIES: " + queryCount + "\r\n" + "\r\n");
								
							}
							queryCount++;
						} insights.clear();
					}
					perspec.clear();
					engine.commitOWL();
					engine.infer();
					engine.commit();
					engine.closeDB();
					if(engine.isConnected()){
						/*DBUG*/System.out.println(engine.getEngineName()+" DIDNT CLOSE");
						//	break;
					} else {
						System.out.println(engine.getEngineName()+" CLOSED");
					}
				}
			}
			System.out.println("~~~~~~~~~~~ DBs Checked ~~~~~~~~~~");
			for(int s = 0; s < listOfDB.length; s++){
				try {
					if(!listOfDB[s].contains("null"))System.out.println(s+1+": "+listOfDB[s]);
				} catch (NullPointerException e){
					//DO Nothing
				}
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
		 * @return Prepped Query
		 */
		private String prepQuery(String query){
			//Clustering; Remove all clustering info so query can be processed
			if(query.contains("+++")){
				String[] clusterRemoval= {
						"+++@NumberOfClusters-OverrideCluster@",
						"+++J48",
						"+++PART",
						"+++DecisionTable", 
						"+++DecisionStump",
						"+++REPTree", 
						"+++LMT", 
						"+++SimpleLogistic", 
						"+++@KNeighbors-K@"}; //Add new Cluster param types here
				for(int i = 0; i < clusterRemoval.length; i++){
					if(query.contains(clusterRemoval[i])){
						query = query.replace(clusterRemoval[i], "");
						System.out.println("Part Removed: "+clusterRemoval[i]);
					}
				}
			}
			
			//Dates; removed them, they are dealt with in the play-sheet and not the engine
			if(query.contains("CONSTRUCT") && query.contains("-Override")){
				String[] dateRemoval = {
						"@Year-OverrideYear@;", 
						"@Month-OverrideMonth@;"
						//Add more here
						};
				for(int i = 0; i < dateRemoval.length; i++){
					if(query.contains(dateRemoval[i])){
						query = query.replace(dateRemoval[i], "");
						System.out.println("Part Removed: "+dateRemoval[i]);
					}
				}
			}
			
			//Params; find and insert query params normally put in by the user
			if(query.contains("@")){
				query = Utility.fillParam(query, Utility.getParams(query));
			}
			return query.trim();
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
			/*DBUG*/System.out.println("CHECKING "+engine.getEngineName());
			
			//Limit the results
			if(query.contains("BINDINGS")){
				query = query.replace("BINDINGS", "LIMIT 3 BINDINGS");
			}
			if(query.contains("LIMIT") || query.contains("limit")){
				//DO Nothing
			} else {query += " LIMIT 3";}
			
			System.out.println(query);
			//get Check
			Boolean result = false;
			Object check = engine.execQuery(query);
			if(check instanceof GraphQueryResult){
				try {
					GraphQueryResult res = (GraphQueryResult) check;
					if(res != null){result = true;}
					res.close();
				} catch (NullPointerException | QueryEvaluationException e){
					//Nothing
				}
			} else if(check instanceof Boolean) {
				try {
					result = (Boolean) check;
				} catch (NullPointerException e){
					//Nothing
				}
			} else if(check instanceof TupleQueryResult){
				try {
					TupleQueryResult res = (TupleQueryResult) check;
					if(res != null){result = true;}
					res.close();
				} catch (NullPointerException | QueryEvaluationException e){
					//Nothing
				}
			} else {
				/*DBUG*/System.out.println("NOT A THING!");
				result = true;
			}
			System.out.println("...");
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
		
		private BigDataEngine megaLoader(String engineLocation){
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
