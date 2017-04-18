package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

/**
 * This reactor is responsible for taking the output of an execution and assigning the result as a variable
 */
public class AssignmentReactor extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		NounMetadata result = planner.getVariableValue("$RESULT");
		if(result != null) {
			//if we get a lambda, just store the final result
			if(result.getNounName().equals(PkslDataTypes.LAMBDA)) {
				result = ((AbstractReactor)result.getValue()).execute();
			} 
			planner.addVariable(operationName.toUpperCase(), result);
			planner.removeVariable("$RESULT");
		} else {
			// if we have a constant value
			// it is just set within the curRow
			// this is because it doesn't produce a result
			// as it doesn't go through a reactor
			// and just adds to the cur row
			
			// we use position 1 because position 0 is a constant 
			// which is stored in curRow and matches operationName
			planner.addVariable(operationName.toUpperCase(), this.curRow.getNoun(1));
		}
		return null;
	}

	private boolean checkVariable(String variableName) {
		//use this method to make sure the variable name doesn't not interfere with frame's headers
		return true;
	}

	@Override
	public List<NounMetadata> getInputs() {
		List<NounMetadata> inputs = super.getInputs();
//		List<NounMetadata> inputs = new Vector<NounMetadata>();
//		// grab all the nouns in the noun store
//		Set<String> nounKeys = this.getNounStore().nounRow.keySet();
//		for(String nounKey : nounKeys) {
//			// grab the genrowstruct for the noun
//			// and add its vector to the inputs list
//			GenRowStruct struct = this.getNounStore().getNoun(nounKey);
//			
//			// add everything except the variable name
//			for(NounMetadata noun : struct.vector) {
//				if(!noun.getValue().toString().equals(this.operationName)) {
//					inputs.add(noun);
//				}
//			}
//		}
//		
//		// we also need to account for some special cases
//		// when we have a filter reactor
//		// it doesn't get added as an op
//		// so we need to go through the child of this reactor
//		// and if it has a filter
//		// add its nouns to the inputs for this reactor
//		for(IReactor child : childReactor) {
//			if(child instanceof FilterReactor) {
//				// child nouns should contain LCOL, RCOL, COMPARATOR
//				Set<String> childNouns = child.getNounStore().nounRow.keySet();
//				for(String cNoun : childNouns) {
//					inputs.addAll(child.getNounStore().getNoun(cNoun).vector);
//				}
//			}
//		}
//		
		
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
		NounMetadata output = new NounMetadata(operationName, PkslDataTypes.CONST_STRING);
		outputs.add(output);
		return outputs;
	}
}
