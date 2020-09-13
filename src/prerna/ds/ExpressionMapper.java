package prerna.ds;

import java.util.Iterator;


public interface ExpressionMapper {
	
	public void setData(Iterator results, String [] columnsUsed, String script);
	
	public boolean hasNext();
	
	public Object next();
	

}
