package prerna.sablecc2.reactor.export;

import prerna.engine.api.IHeadersDataRow;

public interface Formatter {

	public void addData(IHeadersDataRow nextData);
	
	public Object getFormattedData();
}
