package prerna.util.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabase;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.HiveSqlInterpreter;

public class HiveQueryUtil  extends AnsiSqlQueryUtil {

	HiveQueryUtil() {
		super();
		setDbType(RdbmsTypeEnum.HIVE);
	}
	
	HiveQueryUtil(String connectionUrl, String username, String password) {
		super(connectionUrl, username, password);
		setDbType(RdbmsTypeEnum.HIVE);
	}
	
	@Override
	public IQueryInterpreter getInterpreter(IDatabase engine) {
		return new HiveSqlInterpreter(engine);
	}

	@Override
	public IQueryInterpreter getInterpreter(ITableDataFrame frame) {
		return new HiveSqlInterpreter(frame);
	}

}
