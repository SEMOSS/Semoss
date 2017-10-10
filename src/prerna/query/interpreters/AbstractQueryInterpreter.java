package prerna.query.interpreters;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.query.querystruct.QueryStruct2;

public abstract class AbstractQueryInterpreter implements IQueryInterpreter2 {

	protected Logger logger = null;
	
	protected int performCount;
	protected QueryStruct2 qs;
	protected boolean isDistinct;

	public AbstractQueryInterpreter() {
		logger = LogManager.getLogger(this.getClass().getName());
	}
	
	@Override
	public void setQueryStruct(QueryStruct2 qs) {
		this.qs = qs;
	}

	@Override
	public void setDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
	}
	
	@Override
	public boolean isDistinct() {
		return this.isDistinct;
	}
	
	@Override
	public void setLogger(Logger logger) {
		if(logger != null) {
			this.logger = logger;
		}
	}
}
