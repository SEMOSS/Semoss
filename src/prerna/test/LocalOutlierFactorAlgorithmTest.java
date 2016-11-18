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
//import prerna.algorithm.learning.unsupervised.outliers.LOF;
//import prerna.engine.api.ISelectWrapper;
//import prerna.engine.impl.rdf.BigDataEngine;
//import prerna.om.SEMOSSParam;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.util.DIHelper;
//
//public class LocalOutlierFactorAlgorithmTest extends TestCase {
//
//	private LOF alg;
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
//		alg = new LOF();
//	}
//	
//	@Test
//	public void executeTest_withListAndNames(){
//		List<SEMOSSParam> options = alg.getOptions();
//		HashMap<String, Object> selectedOptions = new HashMap<String, Object>();
//		selectedOptions.put(options.get(0).getName(), 0); 
//		selectedOptions.put(options.get(1).getName(), 25);
//		alg.setSelectedOptions(selectedOptions);
//		
//		ITableDataFrame results = alg.runAlgorithm(dataFrame);
//		List<String> changedCols = alg.getChangedColumns();
//		Double[] lopArr = results.getColumnAsNumeric(changedCols.get(0));
//		for(int i = 0; i < lopArr.length; i++){
//			double val = lopArr[i];
//			assertTrue("LOP larger than 0 and less than 1", (val >= 0 && val <=1.0));
//		}
//	}
//	
//}
