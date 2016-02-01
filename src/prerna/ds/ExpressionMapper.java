package prerna.ds;

import java.util.Iterator;

public interface ExpressionMapper {
	
	public void setData(Iterator inputIterator, String [] ids, String script);
	
	public boolean hasNext();
	
	public Object next();
	

}
