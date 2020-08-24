package prerna.query.querystruct;

import java.util.List;
import java.util.Vector;

public class OperationExpression extends GenExpression {
	
	// primarily a container class that keeps all of the union
	public List <String> opNames = new Vector<String>();
	// because it can be union of unions
	public List <GenExpression> operands = new Vector <GenExpression>();
	
}
