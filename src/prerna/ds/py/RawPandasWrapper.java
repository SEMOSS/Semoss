package prerna.ds.py;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;

public class RawPandasWrapper implements IRawSelectWrapper {

	PandasIterator iterator = null;
	
	@Override
	public void execute() {
		// TODO Auto-generated method stub
		

	}

	@Override
	public void setQuery(String query) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getQuery() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanUp() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setEngine(IEngine engine) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return iterator.hasNext();
	}

	@Override
	public IHeadersDataRow next() {
		// TODO Auto-generated method stub
		return iterator.next();
	}

	@Override
	public String[] getHeaders() {
		// TODO Auto-generated method stub
		return iterator.getHeaders();
	}

	@Override
	public SemossDataType[] getTypes() {
		// TODO Auto-generated method stub
		return iterator.colTypes;
	}

	@Override
	public long getNumRows() {
		return iterator.fullData.size();
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	public void setPandasIterator(PandasIterator pi) {
		// TODO Auto-generated method stub
		this.iterator = pi;
	}

	@Override
	public long getNumRecords() {
		// TODO Auto-generated method stub
		return iterator.fullData.size() * getHeaders().length;
	}

}
