package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

/**
 * This reactor is responsible for taking the output of an execution and assigning the result as a variable
 */
public class AssignmentReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		NounMetadata result = planner.getVariable("$RESULT");
		if(result == null) {
			// if we have a constant value
			// it is just set within the curRow
			// this is because it doesn't produce a result
			// as it doesn't go through a reactor
			// and just adds to the cur row
			
			// we use position 1 because position 0 is a constant 
			// which is stored in curRow and matches operationName
			result = this.curRow.getNoun(1);
		}
		
		planner.addVariable(operationName, result);
		return result;
	}
	
	private boolean checkVariable(String variableName) {
		//use this method to make sure the variable name doesn't not interfere with frame's headers
		return true;
	}

	@Override
	public List<NounMetadata> getInputs() {
		List<NounMetadata> inputs = super.getInputs();
		
		// remove the operation name from the inputs
		for(NounMetadata noun : inputs) {
			if(noun.getValue().toString().equals(this.operationName)) {
				inputs.remove(noun);
				break;
			}
		}
		
		return inputs;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		// output is the variable name to be referenced
		List<NounMetadata> outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.operationName, PkslDataTypes.COLUMN);
		outputs.add(output);
		return outputs;
	}
	

	@Override
	public void updatePlan() {
		List<NounMetadata> inputs = getInputs();
		if(inputs != null) {
			if(inputs.size() == 1) {
				// ignore
				// this is like x = x... not really useful
			} else if(inputs.contains(this.operationName)) {
				// we cannot have a cycle in the plan.. must be a DAG
				throw new IllegalArgumentException("Cannot add cycle dependencies in plan.");
			} else {
				super.updatePlan();
			}
		} else {
			super.updatePlan();
		}
	}
}
