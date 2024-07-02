package prerna.util.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.DatabricksSqlInterpreter;
import prerna.util.Constants;

public class DatabricksQueryUtil extends AnsiSqlQueryUtil {

	private static final Logger classLogger = LogManager.getLogger(DatabricksQueryUtil.class);

	private String httpPath = null;
	private String uid = null;
	private String pwd = null;
	
	DatabricksQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.DATABRICKS);
	}
	
	DatabricksQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.DATABRICKS);
	}
	
	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if(configMap == null || configMap.isEmpty()){
			throw new RuntimeException("Configuration map is null or empty");
		}
		
		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		this.httpPath = (String) configMap.get(AbstractSqlQueryUtil.HTTP_PATH);
		this.uid = (String) configMap.get(AbstractSqlQueryUtil.UID);
		this.pwd = (String) configMap.get(AbstractSqlQueryUtil.PWD);
		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);
		this.database = (String) configMap.get(AbstractSqlQueryUtil.DATABASE);
		this.schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		// these are not in connection url, but needed
		if(this.database == null || this.database.isEmpty()) {
			throw new RuntimeException("Must pass in a database");
		}
		if(this.schema == null || this.schema.isEmpty()) {
			throw new RuntimeException("Must pass in a schema");
		}
		return buildConnectionString();
	}
	
	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if(prop == null || prop.isEmpty()){
			throw new RuntimeException("Properties object is null or empty");
		}
		
		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		this.httpPath = (String) prop.get(AbstractSqlQueryUtil.HTTP_PATH);
		this.uid = (String) prop.get(AbstractSqlQueryUtil.UID);
		this.pwd = (String) prop.get(AbstractSqlQueryUtil.PWD);
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);
		this.database = (String) prop.get(AbstractSqlQueryUtil.DATABASE);
		this.schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		// these are not in connection url, but needed
		if(this.database == null || this.database.isEmpty()) {
			throw new RuntimeException("Must pass in a database");
		}
		if(this.schema == null || this.schema.isEmpty()) {
			throw new RuntimeException("Must pass in a schema");
		}
		return buildConnectionString();
	}

	@Override
	public String buildConnectionString() {
		if(this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			return this.connectionUrl;
		}
		
		if(this.hostname == null || this.hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "443";
		}
		
		if(this.httpPath == null || this.httpPath.isEmpty()) {
			throw new RuntimeException("Must pass in http path");
		}
		
		if(this.uid == null || this.uid.isEmpty()){
			throw new RuntimeException("Must pass in UID");
		}
		
		if(this.pwd == null || this.pwd.isEmpty()){
			throw new RuntimeException("Must pass in PWD");
		}
		
		this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port+";httpPath="+this.httpPath+";UID="+this.uid+";PWD="+this.pwd;
		
		if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
			if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
				this.connectionUrl += ";" + this.additionalProps;
			} else {
				this.connectionUrl += this.additionalProps;
			}
		}
		
		return this.connectionUrl;
	}
	
	@Override
	public void enhanceConnection(Connection con) {
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			stmt.execute("use `"+this.database+"`.`"+this.schema+"`");
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	@Override
	public String getDatabaseMetadataCatalogFilter() {
		return this.database;
	}
	
	@Override
	public String getDatabaseMetadataSchemaFilter() {
		return this.schema;
	}
	
	@Override
	public IQueryInterpreter getInterpreter(IDatabaseEngine engine) {
		return new DatabricksSqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new DatabricksSqlInterpreter(frame);
	}
}
