package prerna.sablecc2.reactor.export;

import java.util.HashMap;
import prerna.engine.api.IHeadersDataRow;

public interface Formatter {
		
	public void addData(IHeadersDataRow nextData);
	
	public Object getFormattedData();
	
	void addHeader(String[] keys);
	//void addHeader(String varKey, String type, String vizType);
}
