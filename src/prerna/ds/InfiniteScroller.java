package prerna.ds;

import java.util.HashMap;
import java.util.List;

public interface InfiniteScroller {

	void resetIterators();
	
	void resetTable();
	
	List<HashMap<String, String>> getNextUniqueValues(String columnHeader);
	
	List<HashMap<String, Object>> getNextData();
}
