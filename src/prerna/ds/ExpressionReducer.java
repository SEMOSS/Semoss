package prerna.ds;

import java.util.Iterator;

public interface ExpressionReducer {
	
		public void set(Iterator inputIterator, String [] ids, String script);
	
		public Object reduce();
}
