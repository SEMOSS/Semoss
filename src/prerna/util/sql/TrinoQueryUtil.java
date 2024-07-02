package prerna.util.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.util.Constants;


public class TrinoQueryUtil extends AnsiSqlQueryUtil {

	private static final Logger classLogger = LogManager.getLogger(TrinoQueryUtil.class);

	TrinoQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.TRINO);
	}
	
	TrinoQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.TRINO);
	}
	
	@Override
	public void enhanceConnection(Connection con) {
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			stmt.execute("set path \""+this.schema+"\"");
		} catch (SQLException e) {
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
	}
	
	@Override
	public String setConnectionDetailsfromMap(Map<String, Object> configMap) throws RuntimeException {
		if(configMap == null || configMap.isEmpty()){
			throw new RuntimeException("Configuration map is null or empty");
		}
		
		this.connectionUrl = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(hostname == null || hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		this.port = (String) configMap.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		this.database = (String) configMap.get(AbstractSqlQueryUtil.CATALOG);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.database == null || this.database.isEmpty())
				) {
			throw new RuntimeException("Must pass in the database/catalog");
		}
		
		this.schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(schema == null || schema.isEmpty())
				){
			throw new RuntimeException("Must pass in schema name");
		}
		
		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port+"/"+this.database+"/"+this.schema;

			if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
				if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
					this.connectionUrl += ";" + this.additionalProps;
				} else {
					this.connectionUrl += this.additionalProps;
				}
			}
		}
		
		return this.connectionUrl;
	}

	@Override
	public String setConnectionDetailsFromSMSS(CaseInsensitiveProperties prop) throws RuntimeException {
		if(prop == null || prop.isEmpty()){
			throw new RuntimeException("Properties object is null or empty");
		}
		
		this.connectionUrl = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_URL);
		
		this.hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(hostname == null || hostname.isEmpty())
			) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		this.port = (String) prop.get(AbstractSqlQueryUtil.PORT);
		String port = this.port;
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		this.database = (String) prop.get(AbstractSqlQueryUtil.CATALOG);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.database == null || this.database.isEmpty())
				) {
			throw new RuntimeException("Must pass in the database/catalog");
		}
		
		this.schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(schema == null || schema.isEmpty())
				){
			throw new RuntimeException("Must pass in schema name");
		}
		
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port+"/"+this.database+"/"+this.schema;
		
			if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
				if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
					this.connectionUrl += ";" + this.additionalProps;
				} else {
					this.connectionUrl += this.additionalProps;
				}
			}
		}
		
		return this.connectionUrl;
	}

	@Override
	public String buildConnectionString() {
		if(this.connectionUrl != null && !this.connectionUrl.isEmpty()) {
			return this.connectionUrl;
		}
		
		if(this.hostname == null || this.hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		if(this.database == null || this.database.isEmpty()) {
			throw new RuntimeException("Must pass in the database/catalog");
		}
		
		if(this.schema == null || this.schema.isEmpty()) {
			throw new RuntimeException("Must pass in schema name");
		}
		
		String port = getPort();
		if (port != null && !port.isEmpty()) {
			port = ":" + port;
		} else {
			port = "";
		}
		
		this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+port+"/"+this.database+"/"+this.schema;
		
		if(this.additionalProps != null && !this.additionalProps.isEmpty()) {
			if(!this.additionalProps.startsWith(";") && !this.additionalProps.startsWith("&")) {
				this.connectionUrl += ";" + this.additionalProps;
			} else {
				this.connectionUrl += this.additionalProps;
			}
		}
		
		return this.connectionUrl;
	}
}
