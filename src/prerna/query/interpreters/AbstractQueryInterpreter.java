package prerna.query.interpreters;

import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;

public abstract class AbstractQueryInterpreter implements IQueryInterpreter {

	protected Logger logger = null;
	
	protected int performCount;
	protected AbstractQueryStruct qs;
	protected boolean isDistinct;
	protected Map<String, String> additionalTypes;

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
	
	@Override
	public void setAdditionalTypes(Map<String, String> additionalTypes){
		this.additionalTypes = additionalTypes;
	}
	
}