package prerna.util.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.ImpalaSqlInterpreter;

public class ImpalaQueryUtil extends AnsiSqlQueryUtil {
	
	ImpalaQueryUtil() {
		super();
	}
	
	ImpalaQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
	}
	
	ImpalaQueryUtil(RdbmsTypeEnum dbType, String hostname, String port, String schema, String username, String password) {
		super(dbType, hostname, port, schema, username, password);
	}
	
	@Override
	public IQueryInterpreter getInterpreter(IEngine engine) {
		return new ImpalaSqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new ImpalaSqlInterpreter(frame);
	}

}
