package prerna.engine.api.iterator;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IDatasourceIterator;

public abstract class AbstractDatasourceIterator implements IDatasourceIterator {

	// values for querying
	String query;
	
	// number of return columns
	int numColumns = 0;
	
	// values for return
	String[] headers;
	String[] rawHeaders;
	SemossDataType[] types;

	@Override
	public void setQuery(String query) {
		this.query = query;
	}
	
	@Override
	public String[] getHeaders() {
		return this.headers;
	}
	
	@Override
	public SemossDataType[] getTypes() {
		return this.types;
	}
	
}
