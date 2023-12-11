//package prerna.rdf.main;
//
//import java.io.IOException;
//import java.util.Arrays;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import prerna.engine.api.IHeadersDataRow;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.engine.impl.rdbms.RDBMSNativeEngine;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.test.TestUtilityMethods;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.Utility;
//
//public class TestRDBMSInsights {
//
//	private static final Logger logger = LogManager.getLogger(TestRDBMSInsights.class);
//
//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadDIHelper();
//
//		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\ab.smss";
//		RDBMSNativeEngine coreEngine = new RDBMSNativeEngine();
//		coreEngine.setEngineId("ab");
//		coreEngine.open(engineProp);
//		DIHelper.getInstance().setLocalProperty("ab", coreEngine);
//		
//		String query = "select * from customers";
//		
//		IRawSelectWrapper wrapper = null;
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(coreEngine, query);
//			while(wrapper.hasNext()) {
//				IHeadersDataRow row = wrapper.next();
//				logger.info(Utility.cleanLogString(Arrays.toString(row.getRawValues())));
//			}
//		} catch (Exception e) {
//			logger.error(Constants.STACKTRACE, e);
//		} finally {
//			if(wrapper != null) {
//				try {
//					wrapper.close();
//				} catch (IOException e) {
//					logger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//
//		query = "select * from user_tables";
//		
//		try {
//			wrapper = WrapperManager.getInstance().getRawWrapper(coreEngine, query);
//			while(wrapper.hasNext()) {
//				IHeadersDataRow row = wrapper.next();
//				logger.info(Utility.cleanLogString(Arrays.toString(row.getRawValues())));
//			}
//		} catch (Exception e) {
//			logger.error("StackTrace: ", e);
//		} finally {
//			if (wrapper != null) {
//				try {
//					wrapper.close();
//				} catch (IOException e) {
//					logger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//		
////		//TODO: put in correct path for your database
////		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\test_this.smss";
////		RDBMSNativeEngine coreEngine = new RDBMSNativeEngine();
////		coreEngine.setEngineName("test_this");
////		coreEngine.open(engineProp);
////		DIHelper.getInstance().setLocalProperty("test_this", coreEngine);
////		
////		String query = "SELECT DISTINCT ?relation where "
////				+ "{<http://semoss.org/ontologies/Concept/MOVIE_RESULTS_UNIQUE_ROW_ID/MOVIE_RESULTS> "
////				+ "?relation " 
////				+ "<http://semoss.org/ontologies/Concept/MOVIE_CHARACTERISTICS_UNIQUE_ROW_ID/MOVIE_CHARACTERISTICS> }"
////				+ "";
////		
////		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(coreEngine.getBaseDataEngine(), query);
////		String[] names = wrapper.getDisplayVariables();
////		System.out.println(Arrays.toString(names));
////		// we will keep a set of the concepts such that we know when we need to append a PRIM_KEY_PLACEHOLDER
////		while(wrapper.hasNext()) {
////			ISelectStatement sjss = wrapper.next();
////			List<Object> row = new Vector<Object>();
////			for(int i = 0; i < names.length; i++) {
////				row.add(sjss.getVar(names[i]));
////			}
////			System.out.println(row);
////		}
////		
//		
////		Properties diProp = new Properties();
////		diProp.put("BaseFolder", "C:\\workspace\\Semoss");
////		DIHelper.getInstance().setCoreProp(diProp);
////
////		prerna.engine.impl.rdf.BigDataEngine e = new prerna.engine.impl.rdf.BigDataEngine();
////		String propFile = "C:\\workspace\\Semoss\\db\\Movie_DB.smss";
////		e.open(propFile);
////		
////		Vector<SEMOSSParam> results2 = e.getParams("Movie_DB_18");
////		for(SEMOSSParam r : results2) {
////			System.out.println(r);
////		}
//		
//	}
//	
//}
