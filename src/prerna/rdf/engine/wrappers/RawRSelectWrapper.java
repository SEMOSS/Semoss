package prerna.rdf.engine.wrappers;

import prerna.ds.r.RIterator2;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;

public class RawRSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private RIterator2 output = null;
	private String[] colTypeNames = null;

	@Override
	public void execute() {
		this.output = (RIterator2) this.engine.execQuery(query);
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
	public String[] getDisplayVariables() {
		return displayVar;
	}

	@Override
	public String[] getPhysicalVariables() {
		return var;
	}

	@Override
	public String[] getTypes() {
		return colTypeNames;
	}

	@Override
	public void cleanUp() {
		// need to add this
	}
	
	public void directExecution(RIterator2 output) {
		this.output = output;
		setDefaults();
	}
	
	private void setDefaults() {
		this.var = output.getHeaders();
		this.displayVar = output.getHeaders();
		this.colTypeNames = output.getColTypes();
	}
}
