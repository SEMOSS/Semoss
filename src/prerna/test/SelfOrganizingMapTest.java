//package prerna.test;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.List;
//
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import junit.framework.TestCase;
//import prerna.algorithm.api.ITableDataFrame;
//import prerna.algorithm.learning.unsupervised.som.SOMRoutine;
//import prerna.engine.api.ISelectWrapper;
//import prerna.engine.impl.rdf.BigDataEngine;
//import prerna.math.StatisticsUtilityMethods;
//import prerna.om.SEMOSSParam;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.util.DIHelper;
//
//public class SelfOrganizingMapTest extends TestCase {
//
//	private SOMRoutine alg;
//	private ITableDataFrame dataFrame;
//
//	@BeforeClass 
//	public void setUpOnce() throws IOException{
//		String engineProp = "C:\\workspace\\Semoss\\db\\Movie_DB.smss";
//		BigDataEngine movieDB = new BigDataEngine();
//		movieDB.openDB(engineProp);
//		movieDB.setEngineName("Movie_DB");
//		DIHelper.getInstance().setLocalProperty("Movie_DB", movieDB);
//		
//		String query = "SELECT DISTINCT ?Title ?DomesticRevenue ?InternationalRevenue ?Budget ?RottenTomatoesCritics ?RottenTomatoesAudience "
//				+ "WHERE {{?Title <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Title>}"
//				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/Revenue-Domestic> ?DomesticRevenue }"
//				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/Revenue-International> ?InternationalRevenue}"
//				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/MovieBudget> ?Budget}"
//				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Critics> ?RottenTomatoesCritics }"
//				+ "{?Title <http://semoss.org/ontologies/Relation/Contains/RottenTomatoes-Audience> ?RottenTomatoesAudience }"
//				+ "{?Director <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Director>}"
//				+ "{?Genre <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Genre>}"
//				+ "{?Nominated <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Nominated>}"
//				+ "{?Studio <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Studio>}"
//				+ "{?Title <http://semoss.org/ontologies/Relation/DirectedBy> ?Director}"
//				+ "{?Title <http://semoss.org/ontologies/Relation/BelongsTo> ?Genre}"
//				+ "{?Title <http://semoss.org/ontologies/Relation/Was> ?Nominated}"
//				+ "{?Title <http://semoss.org/ontologies/Relation/DirectedAt> ?Studio}} ORDER BY ?Title  LIMIT 10";
//		
//		System.out.println("Creating Table Data Frame...");
//		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(movieDB, query);
//		dataFrame = wrapper.getTableDataFrame();
//		System.out.println("Data Frame created...");
//
//		movieDB.closeDB();
//	}
//	
//	@Before
//	public void setUp(){
//		alg = new SOMRoutine();
//	}
//	
//	@Test
//	public void executeTest_withNoDataConstructor(){
//		int instanceIndex = 0;
//		double initalRadius = 2.0;
//		double learningRate = 0.07;
//		double tau = 7.5;
//		int maxIterations = 15;
//		
//		List<SEMOSSParam> options = alg.getOptions();
//		HashMap<String, Object> selectedOptions = new HashMap<String, Object>();
//		selectedOptions.put(options.get(0).getName(), instanceIndex); 
//		selectedOptions.put(options.get(1).getName(), initalRadius);
//		selectedOptions.put(options.get(2).getName(), learningRate);
//		selectedOptions.put(options.get(3).getName(), tau);
//		selectedOptions.put(options.get(4).getName(), maxIterations);
//		alg.setSelectedOptions(selectedOptions);
//		ITableDataFrame results = alg.runAlgorithm(dataFrame);
//		int[] numInstancesInGrids = alg.getNumInstancesInGrid();
//
//		int numInstancesInBtree = dataFrame.getUniqueInstanceCount(dataFrame.getColumnHeaders()[instanceIndex]);
//		int numInstances = StatisticsUtilityMethods.getSum(numInstancesInGrids);
//		assertTrue("All instances are in grids...", numInstancesInBtree == numInstances);
//	}
//	
//}
