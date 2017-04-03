package prerna.sablecc2.reactor;

import prerna.sablecc2.om.NounMetadata;

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
		NounMetadata result = this.curRow.getNoun(0);
		planner.addVariable(operationName.toUpperCase(), result);
		return parentReactor;
	}

	@Override
	public void mergeUp() {
		
	}

	@Override
	public void updatePlan() {
		
	}
	
	private boolean checkVariable(String variableName) {
		//use this method to make sure the variable name doesn't not interfere with frame's headers
		return true;
	}
}
