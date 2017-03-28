package prerna.sablecc2.reactor;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

/**
 * 
 * This reactor is responsible for taking the output of an execution and assigning the result as a variable
 * 
 * 
 *
 */
public class AssignmentReactor extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		NounMetadata result = planner.getVariable("$RESULT");
		if(result != null) {
			planner.addVariable(operationName.toUpperCase(), result);
			planner.removeVariable("$RESULT");
		} else {
			// if we have a constant value
			// it is just set within the curRow
			Object constant = this.curRow.get(0);
			PkslDataTypes constantType = this.curRow.getMeta(0);
			result = new NounMetadata(constant, constantType);
			planner.addVariable(operationName.toUpperCase(), result);
		}
		return null;
	}

	@Override
	protected void mergeUp() {
		
	}

	@Override
	protected void updatePlan() {
		
	}
	
	private boolean checkVariable(String variableName) {
		//use this method to make sure the variable name doesn't not interfere with frame's headers
		return true;
	}
}
