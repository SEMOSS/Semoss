package prerna.engine.impl.neo4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.interpreters.CypherInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.util.Constants;

/**
 * This is the connection to a remote neo4j graph database using the jdbc connection
 */
public class Neo4jEngine extends AbstractEngine {
	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEngine.class);
	private Connection conn;

	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		try {
			LOGGER.info("Opening neo4j graph: ");
			Class.forName("org.neo4j.jdbc.bolt.BoltDriver").newInstance();
			String connectionURL = prop.getProperty(Constants.CONNECTION_URL);
			String username = prop.getProperty(Constants.USERNAME);
			String password = prop.getProperty(Constants.PASSWORD);
			LOGGER.info("Connecting to remote graph: " + connectionURL);
			conn = DriverManager.getConnection(connectionURL, username, password);
			LOGGER.info("Done neo4j opening graph: ");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.NEO4J;
	}
	
	@Override
	public Object execQuery(String query) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(query);
			Map<String, Object> map = new HashMap();
			rs = stmt.executeQuery();
			map.put(RDBMSNativeEngine.RESULTSET_OBJECT, rs);
			if(isConnected()){
				map.put(RDBMSNativeEngine.CONNECTION_OBJECT, null);
				map.put(RDBMSNativeEngine.ENGINE_CONNECTION_OBJECT, conn);
			} else {
				map.put(RDBMSNativeEngine.CONNECTION_OBJECT, conn);
				map.put(RDBMSNativeEngine.ENGINE_CONNECTION_OBJECT, null);
			}
			map.put(RDBMSNativeEngine.STATEMENT_OBJECT, stmt);
			return map;
		} catch (Exception e) {
			LOGGER.error("Error executing SQL query = " + query);
			LOGGER.error("Error message = " + e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public IQueryInterpreter getQueryInterpreter() {
		return new CypherInterpreter();
	}
	
	public Connection getGraphDatabaseConnection() {
		return conn;
	}

	@Override
	public void insertData(String query) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeData(String query) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		// TODO Auto-generated method stub
		return null;
	}
	
}

