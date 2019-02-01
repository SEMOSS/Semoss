package prerna.rdf.engine.wrappers;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;

public class RawRSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private RIterator output = null;

	@Override
	public void execute() {
		this.output = (RIterator) this.engine.execQuery(this.query);
		setDefaults();
	}
	
	public void directExecution(RIterator output) {
		this.output = output;
		setDefaults();
	}

	@Override
	public IHeadersDataRow next() {
		return output.next();
	}

	@Override
	public boolean hasNext() {
		return output.hasNext();
	}

	@Override
	public String[] getHeaders() {
		return rawHeaders;
	}

	@Override
	public SemossDataType[] getTypes() {
		return this.types;
	}

	private void setDefaults() {
		this.rawHeaders = output.getHeaders();
		this.headers = this.rawHeaders;
		
		String[] strTypes = output.getColTypes();
		this.types = new SemossDataType[this.rawHeaders.length];
		for(int i = 0; i < this.rawHeaders.length; i++) {
			this.types[i] = SemossDataType.convertStringToDataType(strTypes[i]);
		}
	}
	
	@Override
	public void reset() {
		this.output = (RIterator) this.engine.execQuery(this.query);
	}

	@Override
	public void cleanUp() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public long getNumRows() {
		if(this.numRows == 0) {
			this.numRows = this.output.getNumRows();
		}
		return this.numRows;
	}
	
	@Override
	public long getNumRecords() {
		return getNumRows() * this.headers.length;
	}
}
