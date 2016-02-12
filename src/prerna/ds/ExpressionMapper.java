package prerna.ds;

import java.util.Iterator;
import java.util.Map;


public interface ExpressionMapper {
	
	public void setData(Iterator results, String [] columnsUsed, String script);
	
	public boolean hasNext();
	
	public Object next();
	

}
