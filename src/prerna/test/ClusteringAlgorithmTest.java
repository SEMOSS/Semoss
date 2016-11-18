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
//import prerna.algorithm.learning.unsupervised.clustering.ClusteringRoutine;
//import prerna.engine.api.ISelectWrapper;
//import prerna.engine.impl.rdf.BigDataEngine;
//import prerna.om.SEMOSSParam;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.util.DIHelper;
//
//public class ClusteringAlgorithmTest extends TestCase {
//
//	private ClusteringRoutine cluster;
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
//		String query = "SELECT DISTINCT ?Title ?Genre ?Director ?Nominated ?Studio ?DomesticRevenue ?InternationalRevenue ?Budget ?RottenTomatoesCritics ?RottenTomatoesAudience "
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
//		cluster = new ClusteringRoutine();
//	}
//	
//	@Test
//	public void executeTest(){
//		List<SEMOSSParam> options = cluster.getOptions();
//		HashMap<String, Object> selectedOptions = new HashMap<String, Object>();
//		int numClusters = 2;
//		selectedOptions.put(options.get(0).getName(), numClusters); 
//		selectedOptions.put(options.get(1).getName(), 0);
//		cluster.setSelectedOptions(selectedOptions);
//		
//		ITableDataFrame results = cluster.runAlgorithm(dataFrame);
//
//		// should have exactly 2 clusters
//		// every title should have a cluster
//		Integer[] counts = results.getUniqueInstanceCount();
//
//		assertTrue(counts[1] == numClusters);
//		assertTrue(counts[0] == dataFrame.getUniqueInstanceCount("Title"));
//	}
//}
