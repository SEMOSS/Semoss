package prerna.query.interpreters.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;

public class SnowFlakeSqlInterpreter extends SqlInterpreter {

	public SnowFlakeSqlInterpreter() {
		
	}

	public SnowFlakeSqlInterpreter(IEngine engine) {
		super(engine);
	}
	
	public SnowFlakeSqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}

	
}
