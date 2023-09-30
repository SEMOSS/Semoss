package prerna.engine.impl.neo4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.interpreters.CypherInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * This is the connection to a remote neo4j graph database using the jdbc connection
 */
public class Neo4jEngine extends AbstractDatabaseEngine {

	private static final Logger classLogger = LoggerFactory.getLogger(Neo4jEngine.class);

	protected Map<String, String> typeMap = new HashMap<String, String>();
	protected Map<String, String> nameMap = new HashMap<String, String>();
	protected boolean useLabel = false;
	private Connection conn;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		// get type map
		String typeMapStr = this.smssProp.getProperty(Constants.TYPE_MAP);
		if (typeMapStr != null && !typeMapStr.trim().isEmpty()) {
			try {
				this.typeMap = new ObjectMapper().readValue(typeMapStr, Map.class);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		// get the name map
		String nameMapStr = this.smssProp.getProperty(Constants.NAME_MAP);
		if (nameMapStr != null && !nameMapStr.trim().isEmpty()) {
			try {
				this.nameMap = new ObjectMapper().readValue(nameMapStr, Map.class);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		if (smssProp.containsKey(Constants.TINKER_USE_LABEL)) {
			String booleanStr = smssProp.get(Constants.TINKER_USE_LABEL).toString();
			useLabel = Boolean.parseBoolean(booleanStr);
		}
		this.conn = getGraphDatabaseConnection();
	}

	@Override
	public DATABASE_TYPE getDatabaseType() {
		return IDatabaseEngine.DATABASE_TYPE.NEO4J;
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
			classLogger.error("Error executing cypher query = " + Utility.cleanLogString(query));
			classLogger.error("Error message = " + e.getMessage());
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
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
				classLogger.info("Opening neo4j graph: ");
				Class.forName("org.neo4j.jdbc.bolt.BoltDriver").newInstance();
				String connectionURL = smssProp.getProperty(Constants.CONNECTION_URL);
				String username = smssProp.getProperty(Constants.USERNAME);
				String password = smssProp.getProperty(Constants.PASSWORD);
				classLogger.info("Connecting to remote graph: " + Utility.cleanLogString(connectionURL));
				conn = DriverManager.getConnection(connectionURL, username, password);
				classLogger.info("Done neo4j opening graph: ");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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
	public void close() throws IOException {
		super.close();
		ConnectionUtils.closeConnection(this.conn);
	}

	@Override
	public boolean holdsFileLocks() {
		return false;
	}

}

