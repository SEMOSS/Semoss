package prerna.sablecc2.reactor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class Assimilator extends AbstractReactor {

	// roles of the assimilator is simple, just assimilate an expression and then
	// plug it into the parent
	// filter is a good example of assimilator for example

	private boolean containsStringValue = false;
	
	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		// in the translation flow
		// execute will run and send back
		// the data to set into the parent
		return parentReactor;
	}

	@Override
	public NounMetadata execute() {
		super.execute();
		// evaluate the assimilator as an object
		ClassMaker maker = new ClassMaker();
		maker.addInterface("java.io.Serializable");
		// keep a string to generate the method to execute that will
		// return an object that runs the expression
		StringBuilder expressionBuilder = new StringBuilder();
		expressionBuilder.append("public Object execute(){");
		// we need to grab any variables and define them at the top of the method
		appendVariables(expressionBuilder);
		// now that the variables are defined
		// we just want to add the expression as a return
		if(this.containsStringValue) {
			expressionBuilder.append("return new String(").append(this.signature).append(");}");
		} else {
			// multiply by 1.0 to make sure everything is a double...
			// TODO: really need to expose integer as different from double
			// as a pksl data type
			expressionBuilder.append("return new Double(1.0 * ( ").append(this.signature).append("));}");
		}
		maker.addMethod(expressionBuilder.toString());
		// add a super so we have a base method to execute
		maker.addSuper("prerna.sablecc2.reactor.AssimilatorEvaluator");
		Class newClass = maker.toClass();

		// noun object to return
		// need to cast to get the type of the NounMetadata object
		NounMetadata noun = null;

		try {
			AssimilatorEvaluator newInstance = (AssimilatorEvaluator) newClass.newInstance();
			Object retVal = newInstance.execute();
			// to avoid java error which cannot be caught
			// if the return is null
			// we will throw the exception here
			// which means we could not evaluate the signature
			if(retVal == null) {
				throw new IllegalArgumentException("Error!!! Could not properly evaluate expression = " + this.signature);
			}
			
			if(this.containsStringValue) {
				noun = new NounMetadata(retVal.toString(), PkslDataTypes.CONST_STRING);
			} else {
				noun = new NounMetadata( ((Number) retVal).doubleValue(), PkslDataTypes.CONST_DECIMAL);
			}
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		return noun;
	}

	/**
	 * Append the variables used within the expression
	 * @param expressionBuilder
	 */
	private void appendVariables(StringBuilder expressionBuilder) {
		List<String> inputColumns = curRow.getAllColumns();
		// these input columns should be defined at the beginning of the expression
		// technically, someone can use the same variable multiple times
		// so need to account for this
		// ... just add them to a set and call it a day
		Set<String> uniqueInputs = new HashSet<String>();
		uniqueInputs.addAll(inputColumns);
		for(String input : uniqueInputs) {
			NounMetadata data = planner.getVariableValue(input);
			if(data == null) {
				// this only happens when a variable is being used but isn't defined
				throw new IllegalArgumentException("Undefined variable : " + input);
			}
			PkslDataTypes dataType = data.getNounName();
			// we have a number that is stored
			if(dataType == PkslDataTypes.CONST_DECIMAL) {
				expressionBuilder.append("double ").append(input).append(" = ").append(data.getValue()).append(";");
			}
			// we have a word that is stored
			else if(dataType == PkslDataTypes.CONST_STRING) {
				this.containsStringValue = true;
				expressionBuilder.append("String ").append(input).append(" = \"").append(data.getValue()).append("\";");
			}
			// we have a lambda
			// so we execute and return the operation output
			// TODO: this should no longer be needed since getVariableValue will evaluate a lambda
			else if(dataType == PkslDataTypes.LAMBDA){
				// in case the variable points to another reactor
				// that we need to get the value from
				// evaluate the lambda
				// object better be a reactor to run
				Object rVal = data.getValue();
				if(rVal instanceof IReactor) {
					NounMetadata newNoun = ((IReactor) rVal).execute(); 
					PkslDataTypes newDataType = data.getNounName();
					if(newDataType == PkslDataTypes.CONST_DECIMAL) {
						expressionBuilder.append("double ").append(input).append(" = ").append(newNoun.getValue()).append(";");
					} else if(newDataType == PkslDataTypes.CONST_STRING) {
						this.containsStringValue = true;
						expressionBuilder.append("String ").append(input).append(" = \"").append(newNoun.getValue()).append("\";");
					}
				} else {
					// this should never ever happen....
					throw new IllegalArgumentException("Assimilator cannot handle this type if input");
				}
			} else {
				throw new IllegalArgumentException("Assimilator can currently only handle outputs of scalar variables");
			}
		}
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) {
			return outputs;
		}

		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PkslDataTypes.LAMBDA);
		outputs.add(output);
		return outputs;
	}
}
