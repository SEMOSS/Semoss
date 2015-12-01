package prerna.ds;

import java.util.List;

public interface IBtreeFilterer {

	public void filter(String column, List<Object> objectsToFilter);
	
	public void unfilter(String column);
	
	public void reset();
	
	public Object getFilterModel();
}
