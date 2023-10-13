package prerna.reactor.export;

import java.util.Map;

import prerna.engine.api.IHeadersDataRow;

public interface IFormatter {
		
	public void addData(IHeadersDataRow nextData);
	
	public Object getFormattedData();
	
	void clear();
	
	void setOptionsMap(Map<String, Object> options);
	
	Map<String, Object> getOptionsMap();
	
	String getFormatType();
}
