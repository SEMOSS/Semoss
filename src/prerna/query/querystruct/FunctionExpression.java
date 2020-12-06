package prerna.query.querystruct;

import java.util.List;
import java.util.Vector;

public class FunctionExpression extends GenExpression {
	
	public boolean neutralizeFunction = false; // removes the function from the given column. This only works if your function has a single column
	
	// Function expressions
	public List <GenExpression> expressions = new Vector <GenExpression>();
	
}
