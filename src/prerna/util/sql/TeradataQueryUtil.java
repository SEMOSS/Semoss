package prerna.util.sql;

import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabase;
import prerna.engine.impl.CaseInsensitiveProperties;
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
	
	@Override
	public IQueryInterpreter getInterpreter(IDatabase engine) {
		return new TeradataSqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new TeradataSqlInterpreter(frame);
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
		
		this.database = (String) configMap.get(AbstractSqlQueryUtil.DATABASE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.database == null || this.database.isEmpty())
				){
			throw new RuntimeException("Must pass in database name");
		}
		
		this.additionalProps = (String) configMap.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+"/DATABASE="+this.database;
			
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
		
		this.database = (String) prop.get(AbstractSqlQueryUtil.DATABASE);
		if((this.connectionUrl == null || this.connectionUrl.isEmpty()) && 
				(this.database == null || this.database.isEmpty())
				){
			throw new RuntimeException("Must pass in database name");
		}
		
		this.additionalProps = (String) prop.get(AbstractSqlQueryUtil.ADDITIONAL);

		// do we need to make the connection url?
		if(this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+"/DATABASE="+this.database;
		
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
			throw new RuntimeException("Must pass in database name");
		}
		
		this.connectionUrl = this.dbType.getUrlPrefix()+"://"+this.hostname+"/DATABASE="+this.database;
		
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
	
	@Override
	public StringBuffer addLimitOffsetToQuery(StringBuffer query, long limit, long offset) {

		if(limit > 0) {
			String strquery = query.toString();
			strquery = strquery.replaceFirst("SELECT", "SELECT TOP " + limit + " ");
			query = new StringBuffer();
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