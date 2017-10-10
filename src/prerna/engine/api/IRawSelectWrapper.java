package prerna.engine.api;

import java.util.Iterator;

public interface IRawSelectWrapper extends IEngineWrapper, Iterator {

	IHeadersDataRow next();
	
	String [] getDisplayVariables();
	
	String [] getPhysicalVariables();

	String[] getTypes();

}
