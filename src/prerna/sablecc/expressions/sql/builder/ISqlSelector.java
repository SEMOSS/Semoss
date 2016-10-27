package prerna.sablecc.expressions.sql.builder;

import java.util.List;

public interface ISqlSelector {

	/**
	 * Get the columns used to create the internal piece
	 * @return
	 */
	List<String> getTableColumns();
	
}
