//package prerna.engine.impl;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.Properties;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import prerna.engine.api.IDatabaseEngine;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.engine.api.impl.util.Owler;
//import prerna.engine.impl.rdf.RDFFileSesameEngine;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.util.Constants;
//
//@Deprecated
//public class OwlPrettyPrintFixer {
//
//	private static final Logger classLogger = LogManager.getLogger(OwlPrettyPrintFixer.class);
//
//	@Deprecated
//	public static void fixOwl(Properties prop) {
//		File owlFile = SmssUtilities.getOwlFile(prop);
//		if(owlFile != null && owlFile.exists()) {
//			String conceptualRel = Owler.SEMOSS_URI_PREFIX + Owler.DEFAULT_RELATION_CLASS + "/" + Owler.CONCEPTUAL_RELATION_NAME;
//			
//			// owl is stored as RDF/XML file
//			RDFFileSesameEngine rfse = new RDFFileSesameEngine();
//			rfse.setEngineId(Constants.OWL_TEMPORAL_ENGINE_META);
//			rfse.openFile(owlFile.getAbsolutePath(), null, null);
//	
//			String query = "select ?s ?p ?o where {"
//					+ "bind(<http://www.w3.org/2002/07/owl#Conceptual> as ?p)"
//					+ "{?s ?p ?o}"
//					+ "}";
//			
//			boolean write = false;
//			IRawSelectWrapper wrapper = null;
//			try {
//				wrapper = WrapperManager.getInstance().getRawWrapper(rfse, query);
//				while(wrapper.hasNext()) {
//					write = true;
//					Object[] badTriples = wrapper.next().getRawValues();
//					rfse.doAction(IDatabaseEngine.ACTION_TYPE.REMOVE_STATEMENT, new Object[]{badTriples[0], badTriples[1], badTriples[2], true});
//					rfse.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{badTriples[0], conceptualRel, badTriples[2], true});
//				}
//			} catch (Exception e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			} finally {
//				if(wrapper != null) {
//					try {
//						wrapper.close();
//					} catch (IOException e) {
//						classLogger.error(Constants.STACKTRACE, e);
//					}
//				}
//			}
//			
//			if(write) {
//				try {
//					rfse.exportDB();
//				} catch (Exception e) {
//					classLogger.error(Constants.STACKTRACE, e);
//				}
//			}
//		}
//	}
//
//}
