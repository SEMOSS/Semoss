package prerna.ds;

import java.util.Iterator;
import java.util.Map;

public interface ExpressionReducer {
	
		public void setData(Iterator inputIterator, String [] ids, String script);
	
		public void setData(Map<Object, Object> options);
		
		public Object reduce();
}
