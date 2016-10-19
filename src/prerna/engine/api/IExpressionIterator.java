package prerna.engine.api;

import java.util.Iterator;

public interface IExpressionIterator extends Iterator<Object[]> {

	/**
	 * Shut down the expression
	 */
	void close();

	/**
	 * Get the headers returned from the expression
	 */
	String[] getHeaders();

	/**
	 * This will return the expression string such that it can be combined with other expressions
	 * @return
	 */
	String toString();
	
	/**
	 * Get the join columns (columns existing in the frame) used in the expression
	 * @return
	 */
	String[] getJoinColumns();
	
}
