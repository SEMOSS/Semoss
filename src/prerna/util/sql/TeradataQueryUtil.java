package prerna.util.sql;

import java.util.Map;
import java.util.Properties;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.TeradataSqlInterpreter;

public class TeradataQueryUtil extends AnsiSqlQueryUtil {

	TeradataQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.TERADATA);
	}
	
	TeradataQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.TERADATA);
	}
	
	TeradataQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
	@Override
	public IQueryInterpreter getInterpreter(IEngine engine) {
		return new TeradataSqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new TeradataSqlInterpreter(frame);
	}
	
	@Override
	public String buildConnectionString(Map<String, Object> configMap) throws RuntimeException {
		if(configMap.isEmpty()){
			throw new RuntimeException("Configuration map is empty");
		}

		String connectionString = (String) configMap.get(AbstractSqlQueryUtil.CONNECTION_STRING);
		if(connectionString != null && !connectionString.isEmpty()) {
			return connectionString;
		}
		
		String urlPrefix = this.dbType.getUrlPrefix();
		String hostname = (String) configMap.get(AbstractSqlQueryUtil.HOSTNAME);
		if(hostname == null || hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		String schema = (String) configMap.get(AbstractSqlQueryUtil.SCHEMA);
		if(schema == null || schema.isEmpty()) {
			throw new RuntimeException("Must pass in schema name");
		}
		
		connectionString = urlPrefix+"://"+hostname+"/DATABASE="+schema;
		
		String additonalProperties = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith(";") && !additonalProperties.startsWith("&")) {
				connectionString += ";" + additonalProperties;
			} else {
				connectionString += additonalProperties;
			}
		}
		
		return connectionString;
	}

	@Override
	public String buildConnectionString(Properties prop) throws RuntimeException {
		if(prop == null){
			throw new RuntimeException("Properties ojbect is null");
		}

		String connectionString = (String) prop.get(AbstractSqlQueryUtil.CONNECTION_STRING);
		if(connectionString != null && !connectionString.isEmpty()) {
			return connectionString;
		}
		
		String urlPrefix = this.dbType.getUrlPrefix();
		String hostname = (String) prop.get(AbstractSqlQueryUtil.HOSTNAME);
		if(hostname == null || hostname.isEmpty()) {
			throw new RuntimeException("Must pass in a hostname");
		}
		
		String schema = (String) prop.get(AbstractSqlQueryUtil.SCHEMA);
		if(schema == null || schema.isEmpty()) {
			throw new RuntimeException("Must pass in schema name");
		}
		
		connectionString = urlPrefix+"://"+hostname+"/DATABASE="+schema;
		
		String additonalProperties = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);
		if(additonalProperties != null && !additonalProperties.isEmpty()) {
			if(!additonalProperties.startsWith(";") && !additonalProperties.startsWith("&")) {
				connectionString += ";" + additonalProperties;
			} else {
				connectionString += additonalProperties;
			}
		}
		
		return connectionString;
	}
	
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset) {

		if(limit > 0) {
			String strquery = query.toString();
			strquery=strquery.replaceFirst("SELECT", "SELECT TOP " + limit + " ");
			query = new StringBuilder();
			query.append(strquery);
		}
		
		//TODO there is no offset for now
//		if(offset > 0) {
//			query = query.append(" OFFSET "+offset);
//		}
		return query;
	}
	
	//this creates the temp table to select top from the entire list of distinct selectors. 
	//this is only used with distinct
	public StringBuilder addLimitOffsetToQuery(StringBuilder query, long limit, long offset, String tempTable) {

		if(limit > 0) {
			query=query.insert(0, "SELECT TOP " + limit + " * from (");
			query=query.append(") as "+ tempTable);
		}
		
		//TODO there is no offset for now
//		if(offset > 0) {
//			query = query.append(" OFFSET "+offset);
//		}
		return query;
	}

}