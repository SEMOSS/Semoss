package prerna.engine.impl.neo4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.interpreters.CypherInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;

/**
 * This is the connection to a remote neo4j graph database using the jdbc connection
 */
public class Neo4jEngine extends AbstractEngine {
	private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEngine.class);
	protected Map<String, String> typeMap = new HashMap<String, String>();
	protected Map<String, String> nameMap = new HashMap<String, String>();
	protected boolean useLabel = false;
	private Connection conn;


	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		// get type map
		String typeMapStr = this.prop.getProperty(Constants.TYPE_MAP);
		if (typeMapStr != null && !typeMapStr.trim().isEmpty()) {
			try {
				this.typeMap = new ObjectMapper().readValue(typeMapStr, Map.class);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// get the name map
		String nameMapStr = this.prop.getProperty(Constants.NAME_MAP);
		if (nameMapStr != null && !nameMapStr.trim().isEmpty()) {
			try {
				this.nameMap = new ObjectMapper().readValue(nameMapStr, Map.class);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (prop.containsKey(Constants.TINKER_USE_LABEL)) {
			String booleanStr = prop.get(Constants.TINKER_USE_LABEL).toString();
			useLabel = Boolean.parseBoolean(booleanStr);
		}
		this.conn = getGraphDatabaseConnection();
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
			stmt = getGraphDatabaseConnection().prepareStatement(query);
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
			LOGGER.error("Error executing cypher query = " + query);
			LOGGER.error("Error message = " + e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public IQueryInterpreter getQueryInterpreter() {
		CypherInterpreter interp = new CypherInterpreter(this.typeMap, this.nameMap);
		interp.setUseLabel(this.useLabel);
		return interp;
	}

	public Connection getGraphDatabaseConnection() {
		try {
			if (this.conn == null || this.conn.isClosed()) {
				LOGGER.info("Opening neo4j graph: ");
				Class.forName("org.neo4j.jdbc.bolt.BoltDriver").newInstance();
				String connectionURL = prop.getProperty(Constants.CONNECTION_URL);
				String username = prop.getProperty(Constants.USERNAME);
				String password = prop.getProperty(Constants.PASSWORD);
				LOGGER.info("Connecting to remote graph: " + connectionURL);
				conn = DriverManager.getConnection(connectionURL, username, password);
				LOGGER.info("Done neo4j opening graph: ");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return this.conn;
	}

	@Override
	public void insertData(String query) throws Exception {
		
	}

	@Override
	public void removeData(String query) throws Exception {
		
	}

	@Override
	public void commit() {
		
	}

	@Override
	public Vector<Object> getEntityOfType(String type) {
		return null;
	}
	
	@Override
	public void closeDB() {
		super.closeDB();
		ConnectionUtils.closeConnection(this.conn);
	}
	
	
}

