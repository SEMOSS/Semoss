package prerna.engine.impl.rdbms;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;

import prerna.ds.util.RdbmsQueryBuilder;

public class RdbmsConnectionBuilder {

	enum CONN_TYPE {DIRECT_CONN_URL, BUILD_CONN_URL, BUILD_FROM_FILE, CONNECTION_POOL}

	private Connection conn;
	private BasicDataSource ds;
	
	private CONN_TYPE type;
	private String driver;
	private String userName;
	private String password;
	
	/*
	 * We could have the full URL
	 * or
	 * We could consturct it from its parts
	 */
	private String connectionUrl;
	
	// build the parts
	// host
	private String host;
	// port
	private String port;
	// schema
	private String schema;
	// properties
	private String additionalProps;
	
	// file as an engine
	private String fileLocation;
	// csv columns to types
	private Map<String, String> columnToTypesMap;
	
	public RdbmsConnectionBuilder(CONN_TYPE type) {
		this.type = type;
	}
	
	public Connection build() throws SQLException {
		if(this.type == CONN_TYPE.DIRECT_CONN_URL) {
			this.conn = RdbmsConnectionHelper.getConnection(connectionUrl, userName, password, driver);
		} else if(this.type == CONN_TYPE.BUILD_CONN_URL) {
			this.conn = RdbmsConnectionHelper.buildConnection(driver, host, port, userName, password, schema, additionalProps);
		} else if(this.type == CONN_TYPE.CONNECTION_POOL) {
			this.ds = RdbmsConnectionHelper.getDataSourceFromPool(driver, connectionUrl, userName, password);
			this.conn = this.ds.getConnection();
		} else if(this.type == CONN_TYPE.BUILD_FROM_FILE) {
			this.conn = RdbmsConnectionHelper.getConnection(connectionUrl, userName, password, driver);
			String createQuery = RdbmsQueryBuilder.createTableFromFile(this.fileLocation, this.columnToTypesMap);
			Statement stmt = null;
			try {
				stmt = this.conn.createStatement();
				stmt.execute(createQuery);
			} catch(SQLException e) {
				e.printStackTrace();
			} finally {
				if(stmt != null) {
					stmt.close();
				}
			}
		}
		
		return this.conn;
	}
	
	public BasicDataSource getDataSource() {
		return this.ds;
	}

	public RdbmsConnectionBuilder setConn(Connection conn) {
		this.conn = conn;
		return this;
	}

	public RdbmsConnectionBuilder setDs(BasicDataSource ds) {
		this.ds = ds;
		return this;
	}

	public RdbmsConnectionBuilder setType(CONN_TYPE type) {
		this.type = type;
		return this;
	}

	public RdbmsConnectionBuilder setDriver(String driver) {
		this.driver = driver;
		return this;
	}

	public RdbmsConnectionBuilder setUserName(String userName) {
		this.userName = userName;
		return this;
	}

	public RdbmsConnectionBuilder setPassword(String password) {
		this.password = password;
		return this;
	}

	public RdbmsConnectionBuilder setConnectionUrl(String connectionUrl) {
		this.connectionUrl = connectionUrl;
		return this;
	}

	public RdbmsConnectionBuilder setHost(String host) {
		this.host = host;
		return this;
	}

	public RdbmsConnectionBuilder setPort(String port) {
		this.port = port;
		return this;
	}

	public RdbmsConnectionBuilder setSchema(String schema) {
		this.schema = schema;
		return this;
	}

	public RdbmsConnectionBuilder setAdditionalProps(String additionalProps) {
		this.additionalProps = additionalProps;
		return this;
	}

	public RdbmsConnectionBuilder setFileLocation(String fileLocation) {
		this.fileLocation = fileLocation;
		return this;
	}

	public RdbmsConnectionBuilder setColumnToTypesMap(Map<String, String> columnToTypesMap) {
		this.columnToTypesMap = columnToTypesMap;
		return this;
	}
	
}
