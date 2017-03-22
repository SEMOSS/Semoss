package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;

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
		
		NounMetadata result = planner.getVariable("$RESULT");
		if(result != null) {
			planner.addVariable(operationName.toUpperCase(), result);
			planner.removeVariable("$RESULT");
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
