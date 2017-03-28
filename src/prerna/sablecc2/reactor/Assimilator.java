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
		Vector<String> inputColumns = curRow.getAllColumns();
		String [] allColumns = new String[inputColumns.size()];
		for(int colIndex = 0;colIndex < inputColumns.size(); colIndex++) {
			allColumns[colIndex] = inputColumns.elementAt(colIndex)+"";
		}
		// the expression will just store the entire signature and the columns
		// the columns is added because as we keep going through the parsing
		// columns definitions just get appended into the GenRowStruct curRow 
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
