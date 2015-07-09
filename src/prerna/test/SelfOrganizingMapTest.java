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

import prerna.algorithm.learning.unsupervised.som.*;
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
public class SelfOrganizingMapTest {

	private static String workingDir = System.getProperty("user.dir");
	static int testCounter;
	
	private static SelfOrganizingMap alg;
	
	private static ArrayList<Object[]> list;
	private static String [] names;
	final private static int LIMIT_NUM = 10; 
	
	@BeforeClass 
	public static void setUpOnce(){
		System.out.println("Test Started..");
		//Set the Sudo-Prop
		System.setProperty("file.separator", "/");
		String propFile = workingDir + "/RDF_Map.prop";
		DIHelper.getInstance().loadCoreProp(propFile);
		PropertyConfigurator.configure(workingDir + "/log4j.prop");
		
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
				+ "{?Title <http://semoss.org/ontologies/Relation/DirectedAt> ?Studio}} ORDER BY ?Title  LIMIT "+LIMIT_NUM;
		
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
	public void executeTest_withNoDataConstructor(){
		alg = new SelfOrganizingMap();
		try {
			alg.execute();
		} catch (ArrayIndexOutOfBoundsException e){
			
		}
		
		// grid size is correct
		System.out.println();
		assertTrue("Grid Height", (alg.getGrid().getWidth() == 0));
		assertTrue("Grid Height", (alg.getWidth() == 0));
		assertTrue("Grid Length", (alg.getGrid().getLength() == 0));
		assertTrue("Grid Length", (alg.getLength() == 0));

		// the z-axis across the grid adds up to the number of instances in the
		// data set; z is the number of instances in that grid
		assertTrue("z-axis = num of instances.. ",
				(alg.getNumInstances() == 0));
	}
	
	@Test
	public void executeTest_withDataConstructor(){
		alg = new SelfOrganizingMap(list, names);
		
		//grid size is correct
		assertTrue("Grid Height", (alg.getGrid().getWidth() == 1));
		assertTrue("Grid Height",(alg.getWidth() == 1));
		assertTrue("Grid Length", (alg.getGrid().getLength() == 2));
		assertTrue("Grid Length",(alg.getLength() == 2));
		
		// the z-axis across the grid adds up to the number of instances in the data set; z is the number of instances in that grid
		assertTrue("z-axis = num of instances.. ", (alg.getNumInstances() == LIMIT_NUM));
		
		//check if there are two instances that are exactly the same values;  they should be in the same cell
		boolean check = false;
		
		//TODO
		for(int i = 0; i < alg.getNumInstances(); i++){
			//alg.getGrid().
		}
		
		//assertTrue("Instances with duplicate values expected", check);
		
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
