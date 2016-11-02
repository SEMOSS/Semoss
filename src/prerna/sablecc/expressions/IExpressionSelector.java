package prerna.sablecc.expressions;

import java.util.List;

public interface IExpressionSelector {

	/**
	 * Get the columns used to create the internal piece
	 * @return
	 */
	List<String> getTableColumns();
	
	/**
	 * This will override the existing toString method to get
	 * the appropriate expression string for the selector to execute
	 * @return
	 */
	String toString();
	
	/**
	 * Get the display name for the selector
	 * @return
	 */
	String getName();

}
