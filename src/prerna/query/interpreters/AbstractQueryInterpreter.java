package prerna.query.interpreters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.querystruct.AbstractQueryStruct;

public abstract class AbstractQueryInterpreter implements IQueryInterpreter {

	protected Logger logger = null;
	
	protected int performCount;
	protected AbstractQueryStruct qs;
	protected boolean isDistinct;

	public AbstractQueryInterpreter() {
		logger = LogManager.getLogger(this.getClass().getName());
	}
	
	@Override
	public void setQueryStruct(AbstractQueryStruct qs) {
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