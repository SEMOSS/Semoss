package prerna.query.interpreters.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabase;

public class SnowFlakeSqlInterpreter extends SqlInterpreter {

	public SnowFlakeSqlInterpreter() {
		
	}

	public SnowFlakeSqlInterpreter(IDatabase engine) {
		super(engine);
	}
	
	public SnowFlakeSqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}

	
}
