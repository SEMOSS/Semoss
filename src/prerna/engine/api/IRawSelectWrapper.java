package prerna.engine.api;

import java.util.Iterator;

public interface IRawSelectWrapper extends IEngineWrapper, Iterator {

	public IHeadersDataRow next();
	
	public String [] getDisplayVariables();
	
	public String [] getPhysicalVariables();
	
}
