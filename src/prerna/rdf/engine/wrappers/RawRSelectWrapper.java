package prerna.rdf.engine.wrappers;

import java.io.IOException;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RIterator;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.r.RNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;

public class RawRSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private RIterator output = null;
	
	@Override
	public void execute() throws Exception {
		this.output = (RIterator) this.engine.execQuery(this.query);
		setDefaults();
	}
	
	public void execute(SelectQueryStruct qs) {
		this.output = (RIterator) ((RNativeEngine) this.engine).execQuery(this.query, qs);
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
	public String[] getHeaders() {
		return headers;
	}

	@Override
	public SemossDataType[] getTypes() {
		return this.types;
	}
	
	@Override
	public void reset() throws Exception {
		this.output = (RIterator) this.engine.execQuery(this.query);
	}

	@Override
	public void close() throws IOException {
		this.output.cleanUp();
	}
	
	@Override
	public long getNumRows() {
		if(this.numRows == 0) {
			this.numRows = this.output.getTotalNumRows();
		}
		return this.numRows;
	}
	
	@Override
	public long getNumRecords() {
		return getNumRows() * this.headers.length;
	}
	
	@Override
	public boolean flushable() {
		// WE HAVE ISSUES WITH STRING NA BECOMING NULL
		// WHEN FLUSHING TO JSON
		// SO KEEP AS false
		return false;
	}
	
	@Override
	public String flush() {
		return this.output.getJsonOfResults();
	}
	
	@Override
	public String getQuery() {
		return output.getQuery();
	}
	
	public RIterator getOutput() {
		return this.output;
	}
}
