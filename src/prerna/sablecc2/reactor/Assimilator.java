package prerna.sablecc2.reactor;

import java.util.Vector;

import prerna.sablecc2.om.Expression;

public class Assimilator extends AbstractReactor {
	
	// roles of the assimilator is simple, just assimilate an expression and then
	// plug it into the parent
	// filter is a good example of assimilator for example

	@Override
	public void In() {
		// TODO Auto-generated method stub
        curNoun("all");
	}

	@Override
	public Object Out() {
		// TODO Auto-generated method stub
		Vector inputColumns = curRow.getAllColumns();
		String [] allColumns = new String[inputColumns.size()];
		for(int colIndex = 0;colIndex < inputColumns.size();allColumns[colIndex] = inputColumns.elementAt(colIndex)+"", colIndex++);
		Expression thisExpression = new Expression(signature, allColumns);
		this.parentReactor.getCurRow().addE(thisExpression);
		return parentReactor;
	}

	@Override
	protected void mergeUp() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void updatePlan() {
		// TODO Auto-generated method stub

	}

}
