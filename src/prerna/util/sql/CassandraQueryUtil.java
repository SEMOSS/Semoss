package prerna.util.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.interpreters.sql.CassandraSqlInterpreter;

public class CassandraQueryUtil extends AnsiSqlQueryUtil {

	CassandraQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.CASSANDRA);
	}
	
	CassandraQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.CASSANDRA);
	}
	
	@Override
	public CassandraSqlInterpreter getInterpreter(IDatabaseEngine engine) {
		return new CassandraSqlInterpreter(engine);
	}

	@Override
	public CassandraSqlInterpreter getInterpreter(ITableDataFrame frame) {
		return new CassandraSqlInterpreter(frame);
	}
	
}
