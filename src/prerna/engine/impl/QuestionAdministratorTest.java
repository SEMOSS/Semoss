//package prerna.engine.impl;
//
//import java.io.ByteArrayInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Vector;
//
//import junit.framework.TestCase;
//
//import org.junit.BeforeClass;
//import org.junit.Test;
//import org.openrdf.repository.Repository;
//import org.openrdf.repository.RepositoryConnection;
//import org.openrdf.repository.sail.SailRepository;
//import org.openrdf.rio.RDFFormat;
//import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
//import org.openrdf.sail.memory.MemoryStore;
//
//import prerna.engine.impl.rdf.BigDataEngine;
//import prerna.test.TestUtilityMethods;
//import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
//import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
//import prerna.ui.components.playsheets.datamakers.JoinTransformation;
//import prerna.util.DIHelper;
//
//public class QuestionAdministratorTest extends TestCase {
//
//	private List<DataMakerComponent> dmcList;
//
//	@BeforeClass
//	public void setUp() throws IOException {
//		TestUtilityMethods.loadDIHelper();
//		
//		dmcList = new ArrayList<DataMakerComponent>();
//		String dmc1Query = "SELECT DISTINCT ?TITLE ?STUDIO WHERE { {?TITLE a <http://semoss.org/ontologies/Concept/Title>} {?STUDIO a <http://semoss.org/ontologies/Concept/Studio>} {?TITLE <http://semoss.org/ontologies/Relation/DirectedAt> ?STUDIO} }";
//		DataMakerComponent dmc1 = new DataMakerComponent("Movie_DB", dmc1Query);
//		dmcList.add(dmc1);
//		
//		String dmc2Query = "SELECT DISTINCT ?TITLE ?DIRECTOR WHERE { {?TITLE a <http://semoss.org/ontologies/Concept/Title>} {?DIRECTOR a <http://semoss.org/ontologies/Concept/Director>} {?TITLE <http://semoss.org/ontologies/Relation/DirectedBy> ?DIRECTOR} }";
//		DataMakerComponent dmc2 = new DataMakerComponent("Movie_DB", dmc2Query);
//		ISEMOSSTransformation dmc2Trans = new JoinTransformation();
//		Map<String, Object> paramMap1 = new HashMap<String, Object>();
//		paramMap1.put(JoinTransformation.COLUMN_ONE_KEY, "Title");
//		paramMap1.put(JoinTransformation.COLUMN_TWO_KEY, "Title");
//		paramMap1.put(JoinTransformation.JOIN_TYPE, "inner");		
//		dmc2Trans.setProperties(paramMap1);
//		dmc2.addPostTrans(dmc2Trans);
//		dmcList.add(dmc2);
//	}
//
//	@Test
//	public void testInsightMakeUpStringParsedCorrectly() {
//		String engineProp = "C:\\workspace\\Semoss\\db\\Movie_DB.smss";
//		BigDataEngine movieDB = new BigDataEngine();
//		movieDB.open(engineProp);
//		movieDB.setEngineName("Movie_DB");
//		DIHelper.getInstance().setLocalProperty("Movie_DB", movieDB);
//
//		QuestionAdministrator qa = new QuestionAdministrator(movieDB);
//		String retMakeUp = qa.generateXMLInsightMakeup(dmcList, new Vector());
//		//Test to make sure string produced is valid
//		Exception ex = null;
//		InputStream is = new ByteArrayInputStream(retMakeUp.getBytes() );
//		RepositoryConnection rc = null;
//		try {
//			Repository myRepository;
//			myRepository = new SailRepository(
//					new ForwardChainingRDFSInferencer(
//							new MemoryStore()));
//			myRepository.initialize();
//			rc = myRepository.getConnection();
//			rc.add(is, "semoss.org", RDFFormat.NTRIPLES);
//		} catch(Exception e) {
//			ex = e;
//		}
//
//		assertNull(ex);
//		movieDB.close();
//	}
//
//	// Main method to test adding to an engine
//	public static void main(String[] args) {
//		TestUtilityMethods.loadDIHelper();
//		String engineProp = "C:\\workspace\\Semoss\\db\\Movie_DB.smss";
//		BigDataEngine movieDB = new BigDataEngine();
//		movieDB.open(engineProp);
//		movieDB.setEngineName("Movie_DB");
//		DIHelper.getInstance().setLocalProperty("Movie_DB", movieDB);
//		
//		List<DataMakerComponent> dmcList = new ArrayList<DataMakerComponent>();
//		String dmc1Query = "SELECT DISTINCT ?TITLE ?STUDIO WHERE { {?TITLE a <http://semoss.org/ontologies/Concept/Title>} {?STUDIO a <http://semoss.org/ontologies/Concept/Studio>} {?TITLE <http://semoss.org/ontologies/Relation/DirectedAt> ?STUDIO} }";
//		DataMakerComponent dmc1 = new DataMakerComponent("Movie_DB", dmc1Query);
//		dmcList.add(dmc1);
//		
//		String dmc2Query = "SELECT DISTINCT ?TITLE ?DIRECTOR WHERE { {?TITLE a <http://semoss.org/ontologies/Concept/Title>} {?DIRECTOR a <http://semoss.org/ontologies/Concept/Director>} {?TITLE <http://semoss.org/ontologies/Relation/DirectedBy> ?DIRECTOR} }";
//		DataMakerComponent dmc2 = new DataMakerComponent("Movie_DB", dmc2Query);
//		ISEMOSSTransformation dmc2Trans = new JoinTransformation();
//		Map<String, Object> paramMap1 = new HashMap<String, Object>();
//		paramMap1.put(JoinTransformation.COLUMN_ONE_KEY, "Title");
//		paramMap1.put(JoinTransformation.COLUMN_TWO_KEY, "Title");
//		paramMap1.put(JoinTransformation.JOIN_TYPE, "inner");		
//		paramMap1.put(JoinTransformation.TYPE, JoinTransformation.METHOD_NAME);		
//
//		dmc2Trans.setProperties(paramMap1);
//		dmc2.addPostTrans(dmc2Trans);
//		dmcList.add(dmc2);
//
//		QuestionAdministrator qa = new QuestionAdministrator(movieDB);
//		qa.addQuestion("TEST THIS", "New-Perspective", dmcList, "Grid", "0", "BTreeDataFrame", true, null, null, null);
//		movieDB.close();
//	}
//
//}
