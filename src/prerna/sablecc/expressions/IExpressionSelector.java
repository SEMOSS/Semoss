package prerna.sablecc.expressions;

import java.util.List;

public interface IExpressionSelector {

	/**
	 * Get the columns used to create the internal piece
	 * @return
	 */
	List<String> getTableColumns();

}
