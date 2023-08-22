package prerna.query.interpreters.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;

public class SnowFlakeSqlInterpreter extends SqlInterpreter {

	public SnowFlakeSqlInterpreter() {
		
	}

	public SnowFlakeSqlInterpreter(IDatabaseEngine engine) {
		super(engine);
	}
	
	public SnowFlakeSqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}

	
}
