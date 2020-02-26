package prerna.util.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.HiveSqlInterpreter;
import prerna.query.interpreters.sql.TeradataSqlInterpreter;

public class HiveQueryUtil  extends AnsiSqlQueryUtil {

	HiveQueryUtil() {
		super();
	}
	
	HiveQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	HiveQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
	@Override
	public IQueryInterpreter getInterpreter(IEngine engine) {
		return new HiveSqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new HiveSqlInterpreter(frame);
	}
	
}
