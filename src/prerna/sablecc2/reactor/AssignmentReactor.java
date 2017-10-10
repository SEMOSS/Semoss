package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;

/**
 * This reactor is responsible for taking the output of an execution and assigning the result as a variable
 */
public class AssignmentReactor extends AbstractReactor implements JavaExecutable {
	
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
		NounMetadata output = new NounMetadata(this.operationName, PixelDataType.COLUMN);
		outputs.add(output);
		return outputs;
	}
	

	@Override
	public void updatePlan() {
		super.updatePlan();
		this.planner.addProperty(signature, "JAVA_SIGNATURE", getJavaSignature());
		this.planner.addProperty(signature, "REACTOR_TYPE", this.reactorName);
	}

	@Override
	public String getJavaSignature() {
		StringBuilder javaSignature = new StringBuilder();
		javaSignature.append(this.operationName+" = ");
		List<NounMetadata> inputs = this.getJavaInputs();
		NounMetadata assignmentNoun = inputs.get(0);
		Object assignmentInput = assignmentNoun.getValue();
		String argument;
		if(assignmentInput instanceof JavaExecutable) {
			argument = ((JavaExecutable)assignmentInput).getJavaSignature();
		} else {
			if(assignmentNoun.getNounType() == PixelDataType.CONST_STRING) {
				argument = "\""+assignmentInput.toString() +"\"";
			} else {
				argument = assignmentInput.toString();
			}
		}
		javaSignature.append(argument);
		
		if(argument.equals(this.operationName)) {
			return "";
		}
		return javaSignature.toString();
	}

	@Override
	public List<NounMetadata> getJavaInputs() {
		List<NounMetadata> inputs = new Vector<NounMetadata>();
		// grab all the nouns in the noun store
		Set<String> nounKeys = this.getNounStore().nounRow.keySet();
		for(String nounKey : nounKeys) {
			// grab the genrowstruct for the noun
			// and add its vector to the inputs list
			GenRowStruct struct = this.getNounStore().getNoun(nounKey);
			inputs.addAll(struct.vector);
		}
		inputs.remove(0);
		return inputs;
	}
	
	@Override
	public String getReturnType() {
		Object returnObj = getJavaInputs().get(0).getValue();
		if(returnObj instanceof JavaExecutable) {
			return ((JavaExecutable)returnObj).getReturnType();
		}
		return returnObj.toString();
	}
	
	public static boolean isSimpleAssignment(String pksl){
		String[] strs = pksl.split("=");
		if(strs[1].trim().matches("\\(\\s*" + strs[0].trim() + "\\s*\\);")){
			return true;
		}
		return false;
	} 
}
