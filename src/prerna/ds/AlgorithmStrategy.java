package prerna.ds;

import java.util.Iterator;

public interface AlgorithmStrategy {
	
	public Object execute();
	
	public void setData(Iterator inputIterator, String[] ids, String prop);

}
