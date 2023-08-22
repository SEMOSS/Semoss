package prerna.util.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.ImpalaSqlInterpreter;

public class ImpalaQueryUtil extends AnsiSqlQueryUtil {
	
	ImpalaQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.IMPALA);
	}
	
	ImpalaQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.IMPALA);
	}
	
	@Override
	public IQueryInterpreter getInterpreter(IDatabaseEngine engine) {
		return new ImpalaSqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new ImpalaSqlInterpreter(frame);
	}
	
}
