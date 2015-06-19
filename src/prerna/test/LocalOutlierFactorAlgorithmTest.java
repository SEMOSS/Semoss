package prerna.test;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import prerna.algorithm.learning.unsupervised.clustering.LocalOutlierFactorAlgorithm;
import prerna.algorithm.learning.unsupervised.clustering.SelfOrganizingMap;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
*
* @author  August Bender
* @version 1.0
* @since   06-11-2015 
* Questions? Email abender@deloitte.com
*/
public class LocalOutlierFactorAlgorithmTest {

	private static String workingDir = System.getProperty("user.dir");
	static int testCounter;
	
	private static LocalOutlierFactorAlgorithm alg;
	
	private static ArrayList<Object[]> list;
	private static String [] names;
	
	private static ArrayList<Object[]> masterTable;
	private static String[] varNames;
	
	@BeforeClass 
	public static void setUpOnce(){
		//Set the Sudo-Prop
		System.setProperty("file.separator", "/");
		String propFile = workingDir + "/RDF_Map.prop";
		DIHelper.getInstance().loadCoreProp(propFile);
		PropertyConfigurator.configure(workingDir + "/log4j.prop");
		
		System.out.println("Test Started..");
		
		
		String engineLocation = workingDir + "//db//Movie_DB.smss";
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
				+ "{?Title <http://semoss.org/ontologies/Relation/DirectedAt> ?Studio}} ORDER BY ?Title  LIMIT 10";
		
		BigDataEngine engine = loadEngine(engineLocation);
		
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
		masterTable = list;
		varNames = names;
		
		engine.commitOWL();
		engine.commit();
		engine.closeDB();
	}
	
	@Before
	public void setUp(){
		testCounter++;
		System.out.println("Test " + testCounter + " starting..");
		
	}
	
	@After
	public void tearDown(){
		System.out.println("Test " + testCounter + " ended..");
	}
	
	@AfterClass
	public static void finalTearDown(){
		System.out.println("Class Tear Down ...");
	}
	
	@Test
	public void executeTest_withListAndNames(){
		alg = new LocalOutlierFactorAlgorithm(list, names);
		try {
			alg.execute();
		} catch (ArrayIndexOutOfBoundsException e){
			
		}
		
		for(int i = 0; i < alg.getLrd().length; i++){
			assertTrue("LRD results", (alg.getLrd()[i] == 0.0));
		}
		
		for(int i = 0; i < alg.getLof().length; i++){
			assertTrue("LRD results", (alg.getLof()[i] == Double.POSITIVE_INFINITY));
		}
		
		for(int i = 0; i < alg.getLop().length; i++){
			assertTrue("LRD results", ((alg.getLop()[i] == 1.0) || (alg.getLop()[i] == 0.0)));
		}
	}
	
	@Test
	public void executeTest_withMasterTableAndMasterNames(){
		alg = new LocalOutlierFactorAlgorithm(list, names);
		
		masterTable = alg.getMasterTable();
		varNames = alg.getNames();
		alg = new LocalOutlierFactorAlgorithm(masterTable, varNames);
		try {
			alg.execute();
		} catch (ArrayIndexOutOfBoundsException e){
			
		}
		
		for(int i = 0; i < alg.getLrd().length; i++){
			assertTrue("LRD results", (alg.getLrd()[i] == 0.0));
		}
		
		for(int i = 0; i < alg.getLof().length; i++){
			assertTrue("LRD results", (alg.getLof()[i] == Double.POSITIVE_INFINITY));
		}
		
		for(int i = 0; i < alg.getLop().length; i++){
			assertTrue("LRD results", ((alg.getLop()[i] == 1.0) || (alg.getLop()[i] == 0.0)));
		}
	}
	
	/** Loads an Engine based on it's .smss file path
	 * 
	 * @param engineLocation
	 * 
	 * @return loaded BigDataEngine
	 */
	private static BigDataEngine loadEngine(String engineLocation){
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
