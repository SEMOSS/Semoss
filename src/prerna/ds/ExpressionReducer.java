package prerna.ds;

import java.util.Iterator;
import java.util.Map;

public interface ExpressionReducer {

	void setData(Iterator inputIterator, String [] ids, String script);

	void setData(Map<Object, Object> options);

	Object reduce();
}
