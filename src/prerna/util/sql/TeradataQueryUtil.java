package prerna.util.sql;

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