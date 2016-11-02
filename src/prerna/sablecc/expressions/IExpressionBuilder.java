package prerna.sablecc.expressions;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;

public interface IExpressionBuilder {

	/**
	 * Get the data frame associated with the expression
	 * @return
	 */
	ITableDataFrame getFrame();
	
	/**
	 * Add a selector for the output of the operation
	 * @param selector
	 */
	void addSelector(IExpressionSelector selector);
	
	/**
	 * Add a selector at a given position of the return
	 * @param index
	 * @param selector
	 */
	void addSelector(int index, IExpressionSelector selector);
	
	/**
	 * Get the list of selector objects
	 * @return
	 */
	List<IExpressionSelector> getSelectors();
	
	/**
	 * Get the number of selectors
	 * @return
	 */
	int numSelectors();
	
	/**
	 * Get the selector object at a specific index
	 * @param index
	 * @return
	 */
	IExpressionSelector getSelector(int index) ;
	
	/**
	 * Get the names of the selectors as returned by the operation
	 * @return
	 */
	public List<String> getSelectorNames();
	
	/**
	 * Get the last selector added
	 * @return
	 */
	IExpressionSelector getLastSelector() ;
	
	/**
	 * Replace an existing selector with a new selector
	 * Used when we are building to modify a selector
	 * @param previousSelector
	 * @param newSelector
	 */
	void replaceSelector(IExpressionSelector previousSelector, IExpressionSelector newSelector);
	
	/**
	 * Remove an existing selector
	 * @param previousSelector
	 */
	void removeSelector(IExpressionSelector selector);
	
	/**
	 * Add a group by for the expression
	 * @param groupBySelector
	 */
	void addGroupBy(IExpressionSelector groupBySelector);
	
	/**
	 * Get the columns names used in the group by
	 * @return
	 */
	List<String> getGroupByColumns();
	
	/**
	 * Get the columns objects used in the group by
	 * @return
	 */
	List<IExpressionSelector> getGroupBySelectors();
	
	/**
	 * Get the string for the selectors
	 * @return
	 */
	String getSelectorString();
	
	/**
	 * Get the string for the group by
	 * @return
	 */
	String getGroupByString();
	
	/**
	 * Get a list of all the table columns used
	 * @return
	 */
	List<String> getAllTableColumnsUsed();
	
	/**
	 * method to build the expression
	 * @return
	 */
	String toString();
	
	/**
	 * Determines if the expression is actually just a scalar response
	 * @return
	 */
	boolean isScalar();
	
	/**
	 * Get the scalar value of the expression
	 * @return
	 */
	Object getScalarValue();
}
