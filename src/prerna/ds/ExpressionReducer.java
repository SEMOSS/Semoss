package prerna.ds;

import java.util.Iterator;

public interface ExpressionReducer {
	
		public void setData(Iterator inputIterator, String [] ids, String script);
	
		public Object reduce();
}
