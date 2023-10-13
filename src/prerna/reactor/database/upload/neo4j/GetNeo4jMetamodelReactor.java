package prerna.reactor.database.upload.neo4j;
//package prerna.sablecc2.reactor.database.upload.neo4j;
//
//import java.io.File;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.HashMap;
//import java.util.Map;
//
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.factory.GraphDatabaseFactory;
//
//import prerna.sablecc2.om.GenRowStruct;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.PixelOperationType;
//import prerna.sablecc2.om.ReactorKeysEnum;
//import prerna.sablecc2.om.execptions.SemossPixelException;
//import prerna.sablecc2.om.nounmeta.NounMetadata;
//import prerna.sablecc2.reactor.AbstractReactor;
//import prerna.sablecc2.reactor.masterdatabase.util.GenerateMetamodelLayout;
//import prerna.util.ConnectionUtils;
//import prerna.util.Constants;
//import prerna.util.GraphUtility;
//import prerna.util.upload.UploadInputUtility;
//
///*
// * Since neo4j-tinkerpop-api-impl is no longer supported
// * Removing logic around interacting with neo4j through gremlin
// */
//
//public class GetNeo4jMetamodelReactor extends AbstractReactor {
//
//	public GetNeo4jMetamodelReactor() {
//		this.keysToGet = new String[] { ReactorKeysEnum.CONNECTION_STRING_KEY.getKey(),
//				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(),
//				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(),
//				ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.USE_LABEL.getKey() };
//	}
//
//	@Override
//	public NounMetadata execute() {
//		organizeKeys();
//		Map<String, Object> retMap = new HashMap<String, Object>();
//		String filePath = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
//		
//		boolean useLabel = useLabel();
//		String graphTypeId = this.keyValue.get(ReactorKeysEnum.GRAPH_TYPE_ID.getKey());
//		if (!useLabel) {
//			if (graphTypeId == null) {
//				SemossPixelException exception = new SemossPixelException(
//						new NounMetadata("Requires graphTypeId to get graph metamodel.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
//				exception.setContinueThreadOfExecution(false);
//				throw exception;
//			}
//		}
//		if (filePath != null) {
//			filePath = UploadInputUtility.getFilePath(this.store, this.insight);
//			try {
//				GraphDatabaseService dbService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(filePath));
//				if (useLabel) {
//					retMap = GraphUtility.getMetamodel(dbService);
//				} else {
//					retMap = GraphUtility.getMetamodel(dbService, graphTypeId);
//				}
//				dbService.shutdown();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		} else {
//			// this is if we want to get the metamodel for a remote graph
//			String connectionStringKey = this.keyValue.get(this.keysToGet[0]);
//			if (connectionStringKey == null) {
//				String msg = "Requires a Connection URL (e.g. bolt://localhost:9999) to get graph metamodel";
//				SemossPixelException exception = new SemossPixelException(
//						new NounMetadata(msg, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
//				exception.setContinueThreadOfExecution(false);
//				throw exception;
//			}
//			// Prepend jdbc keyword for neo4j
//			connectionStringKey = "jdbc:neo4j:" + connectionStringKey;
//			String username = this.keyValue.get(this.keysToGet[1]);
//			if (username == null) {
//				SemossPixelException exception = new SemossPixelException(
//						new NounMetadata("Requires username to get graph metamodel.", PixelDataType.CONST_STRING,
//								PixelOperationType.ERROR));
//				exception.setContinueThreadOfExecution(false);
//				throw exception;
//			}
//
//			String password = this.keyValue.get(this.keysToGet[2]);
//			if (password == null) {
//				SemossPixelException exception = new SemossPixelException(
//						new NounMetadata("Requires password to get graph metamodel.", PixelDataType.CONST_STRING,
//								PixelOperationType.ERROR));
//				exception.setContinueThreadOfExecution(false);
//				throw exception;
//			}
//
//			Connection conn = null;
//			try {
//				// Must specify name of Neo4j Bolt Class for JDBC to find the
//				// right
//				// driver to connect to DB
//				// TODO jdbc::neo4j needs to be a constant
//				Class.forName("org.neo4j.jdbc.bolt.BoltDriver");
//				// Create Connection
//				conn = DriverManager.getConnection(connectionStringKey, username, password);
//				// Get Metamodel
//				if (useLabel) {
//					retMap = GraphUtility.getMetamodel(conn);
//				} else {
//					retMap = GraphUtility.getMetamodel(conn, graphTypeId);
//				}
//
//			} catch (ClassNotFoundException e) {
//				// If org.neo4j.jdbc.bolt.BoltDriver not found
//				e.printStackTrace();
//			} catch (SQLException e) {
//				// From a database access error or if we called on a closed
//				// connection
//				e.printStackTrace();
//			} finally {
//				ConnectionUtils.closeConnection(conn);
//			}
//		}
//
//		// position tables in metamodel to be spaced and not overlap
//		Map<String, Map<String, Double>> nodePositionMap = GenerateMetamodelLayout.generateMetamodelLayoutForGraphDBs(retMap);
//		retMap.put(Constants.POSITION_PROP, nodePositionMap);
//
//		return new NounMetadata(retMap, PixelDataType.MAP);
//	}
//	
//	/**
//	 * Query the external db with a label to get the node
//	 * 
//	 * @return
//	 */
//	private boolean useLabel() {
//		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.USE_LABEL.getKey());
//		if (grs != null && !grs.isEmpty()) {
//			return (boolean) grs.get(0);
//		}
//		return false;
//	}
//}
