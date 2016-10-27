package prerna.sablecc.expressions;

import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;

public interface IExpressionIterator extends Iterator<Object[]> {

	/**
	 * This will be used to generate the full expression to execute
	 */
	void generateExpression();
	
	/**
	 * This will actually run the expression to execute
	 */
	void runExpression();
	
	/**
	 * Shut down the expression
	 */
	void close();

	/**
	 * Get the headers returned from the expression
	 */
	String[] getHeaders();
	
	/**
	 * Set the headers returned from the expression
	 */
	void setHeaders(String[] headers);

	/**
	 * Get the join columns (columns existing in the frame) used in the expression
	 * @return
	 */
	String[] getJoinColumns();
	
	/**
	 * Set the join columns used in the expression
	 * @param joinCols
	 */
	void setJoinCols(String[] joinCols);
	
	/**
	 * Get the group columns used in the operation
	 * @return
	 */
	String[] getGroupColumns();
	
	/**
	 * Set the group columns used in teh operation
	 * @param groupColumns
	 */
	void setGroupColumns(String[] groupColumns);

	
	/**
	 * Get the column name created by the expression
	 * @return
	 */
	List<String> getNewColumnName();
	
	/**
	 * Set the column name created by the expression
	 * @param newColumnName
	 */
	void setNewColumnName(List<String> newColumnName);
	
	/**
	 * Set the data farme to run the expression on
	 * @param frame
	 */
	void setFrame(ITableDataFrame frame);

	/**
	 * Set the derived expression column
	 * Not intended to set the entire sql to execute
	 * @param sqlExpression
	 */
	void setExpression(List<String> sqlExpression);
	
	/**
	 * The expression used for the new column being generated
	 * @return
	 */
	List<String> getExpression();
	
	/**
	 * This will the same thing as getExpression
	 * @return
	 */
	String toString();
	
}
