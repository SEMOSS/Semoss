package prerna.engine.api.iterator;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RIterator;
import prerna.engine.api.IHeadersDataRow;

public class RDatasourceIterator extends AbstractDatasourceIterator {

	private RDataTable dt;
	private RIterator output;
	
	public RDatasourceIterator(RDataTable dt) {
		this.dt = dt;
	}
	
	@Override
	public void execute() {
		this.output = new RIterator(this.dt.getBuilder(), query);
		this.headers = output.getHeaders();
		this.rawHeaders = this.headers;
		this.numColumns = this.rawHeaders.length;
		
		String[] strTypes = output.getColTypes();
		this.types = new SemossDataType[this.numColumns];
		for(int i = 0; i < this.numColumns; i++) {
			this.types[i] = SemossDataType.convertStringToDataType(strTypes[i]);
		}
	}
	
	@Override
	public boolean hasNext() {
		return output.hasNext();
	}
	
	@Override
	public IHeadersDataRow next() {
		return output.next();
	}

	@Override
	public long getNumRecords() {
		return this.output.getNumRows();
	}

	@Override
	public void reset() {
		this.output = new RIterator(this.dt.getBuilder(), query);
	}

	@Override
	public void cleanUp() {
		// TODO Auto-generated method stub
		
	}
	
}
