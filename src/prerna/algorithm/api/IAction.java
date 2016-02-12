package prerna.algorithm.api;

// basic actions to be done 
/*
 * col import
 * col join
 * row.split 
 * table.split
 * 
 *  
 */
public interface IAction {
	
	public void set(String key, Object value);

	// process a particular cell
	public void processCell(String nodeName, Object data);
	
	// process a row
	public void processRow(String nodeName, Object data);
	
	// process a table
	public void processTable(String nodeName, Object data);
}
