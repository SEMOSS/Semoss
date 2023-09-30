//package prerna.engine.impl.neo4j;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Vector;
//
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.Result;
//import org.neo4j.graphdb.Transaction;
//import org.neo4j.graphdb.factory.GraphDatabaseFactory;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import prerna.engine.api.IDatabaseEngine;
//import prerna.engine.impl.AbstractDatabaseEngine;
//import prerna.engine.impl.SmssUtilities;
//import prerna.query.interpreters.CypherInterpreter;
//import prerna.query.interpreters.IQueryInterpreter;
//import prerna.util.Constants;
//import prerna.util.Utility;
//
///*
// * Since neo4j-tinkerpop-api-impl is no longer supported
// * Removing logic around interacting with neo4j through gremlin
// */
//
//public class Neo4jEmbeddedEngine extends AbstractDatabaseEngine {
//	
//	private static final Logger classLogger = LoggerFactory.getLogger(Neo4jEmbeddedEngine.class);
//	
//	private GraphDatabaseService db;
//	protected Map<String, String> typeMap = new HashMap<String, String>();
//	protected Map<String, String> nameMap = new HashMap<String, String>();
//	protected boolean useLabel = false;
//
//	@Override
//	public void open(Properties smssProp) throws Exception {
//		super.open(smssProp);
//		// get type map
//		String typeMapStr = this.smssProp.getProperty(Constants.TYPE_MAP);
//		if (typeMapStr != null && !typeMapStr.trim().isEmpty()) {
//			try {
//				this.typeMap = new ObjectMapper().readValue(typeMapStr, Map.class);
//			} catch (IOException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//		}
//		// get the name map
//		String nameMapStr = this.smssProp.getProperty(Constants.NAME_MAP);
//		if (nameMapStr != null && !nameMapStr.trim().isEmpty()) {
//			try {
//				this.nameMap = new ObjectMapper().readValue(nameMapStr, Map.class);
//			} catch (IOException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//		}
//		if (smssProp.containsKey(Constants.TINKER_USE_LABEL)) {
//			String booleanStr = smssProp.get(Constants.TINKER_USE_LABEL).toString();
//			useLabel = Boolean.parseBoolean(booleanStr);
//		}
//
//		String neo4jFile = SmssUtilities.getNeo4jFile(smssProp).getAbsolutePath();
//		try {
//			classLogger.info("Opening neo4j graph: " + Utility.cleanLogString(neo4jFile));
//			db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(neo4jFile));
//			classLogger.info("Done neo4j opening graph: " + Utility.cleanLogString(neo4jFile));
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//	}
//
//	@Override
//	public DATABASE_TYPE getDatabaseType() {
//		return IDatabaseEngine.DATABASE_TYPE.NEO4J_EMBEDDED;
//	}
//
//	@Override
//	public Object execQuery(String query) {
//		try {
//			Transaction ignored = db.beginTx();
//			Result result = db.execute(query);
//			List<String> columns = result.columns();
//			System.out.println("***************************************************");
//
//			while (result.hasNext()) {
//				Map<String, Object> row = result.next();
//				for (String col : columns) {
//					Object val = row.get(col);
//					System.out.println("Column:" + col + " Value: " + val);
//				}
//				System.out.println("***************************************************");
//				for (int i = 0; i < 5; i++) {
//					System.out.println("***************************************************");
//
//				}
//			}
//			result.close();
//			ignored.close();
//
//		} finally {
//
//		}
//
//		return null;
//	}
//
//	@Override
//	public IQueryInterpreter getQueryInterpreter() {
//		CypherInterpreter interp = new CypherInterpreter(this.typeMap, this.nameMap);
//		interp.setUseLabel(this.useLabel);
//		return interp;
//	}
//
//	public GraphDatabaseService getGraphDatabaseService() {
//		return db;
//	}
//
//	@Override
//	public boolean holdsFileLocks() {
//		return true;
//	}
//	
//	@Override
//	public void insertData(String query) throws Exception {
//
//	}
//
//	@Override
//	public void removeData(String query) throws Exception {
//
//	}
//	
//	@Override
//	public void commit() {
//
//	}
//
//	@Override
//	public Vector<Object> getEntityOfType(String type) {
//		return null;
//	}
//}