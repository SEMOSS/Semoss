package prerna.sablecc2.reactor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.StringUtils;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class PowAssimilator extends Assimilator {

	private static final Logger LOGGER = LogManager.getLogger(PowAssimilator.class.getName());

	private boolean allIntValue = true;

	private String lSignature;
	private String rSignature;
	
	@Override
	public void modifySignature(String stringToFind, String stringReplacement) {
		// for this special reactor
		// we need to run this on both the 
		// left hand side and
		// right hand size
		LOGGER.debug("Original left signature value = " + this.lSignature);
		this.lSignature = StringUtils.replaceOnce( this.lSignature, stringToFind, stringReplacement);
		LOGGER.debug("New left signature value = " + this.lSignature);
		
		LOGGER.debug("Original right signature value = " + this.rSignature);
		this.rSignature = StringUtils.replaceOnce( this.rSignature, stringToFind, stringReplacement);
		LOGGER.debug("New right signature value = " + this.rSignature);
	}

	@Override
	public NounMetadata execute() {
		modifySignatureFromLambdas();
		// in order to ensure that we are properly creating the object as a double
		// we need to remove any wrapping parenthesis such that we can 
		// properly make it a double by multiplying by 1.0
		while(lSignature.startsWith("(") && lSignature.endsWith(")")) {
			lSignature = lSignature.substring(1, lSignature.length()-1).trim();
		}
		while(rSignature.startsWith("(") && rSignature.endsWith(")")) {
			rSignature = rSignature.substring(1, rSignature.length()-1).trim();
		}
		
		// need to see if we are dealing with any non-integer values
		if(this.curRow.getNounsOfType(PkslDataTypes.CONST_DECIMAL).size() > 0) {
			this.allIntValue = false;
		}

		// evaluate the assimilator as an object
		ClassMaker maker = new ClassMaker();
		// keep a string to generate the method to execute that will
		// return an object that runs the expression
		StringBuilder expressionBuilder = new StringBuilder();
		expressionBuilder.append("public Object execute(){");
		// we need to grab any variables and define them at the top of the method
		appendVariables(expressionBuilder);
		// now that the variables are defined
		// we just want to add the expression as a return
		expressionBuilder.append("return new Double(Math.pow( 1.0 * " + lSignature + ", 1.0 * " + rSignature + "));}");
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
			
			if(this.allIntValue) {
				Number result = (Number) retVal;
				if(result.doubleValue() == Math.rint(result.doubleValue())) {
					noun = new NounMetadata( ((Number) retVal).intValue(), PkslDataTypes.CONST_INT);
				} else {
					// not a valid integer
					// return as a double
					noun = new NounMetadata( ((Number) retVal).doubleValue(), PkslDataTypes.CONST_DECIMAL);
				}
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
			PkslDataTypes dataType = data.getNounType();
			// we have a double that is stored
			if(dataType == PkslDataTypes.CONST_DECIMAL) {
				expressionBuilder.append("double ").append(input).append(" = ").append(data.getValue()).append(";");
			}
			// we have an integer that is stored
			else if(dataType == PkslDataTypes.CONST_INT) {
				expressionBuilder.append("int ").append(input).append(" = ").append(data.getValue()).append(";");
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
					PkslDataTypes newDataType = data.getNounType();
					if(newDataType == PkslDataTypes.CONST_DECIMAL) {
						expressionBuilder.append("double ").append(input).append(" = ").append(newNoun.getValue()).append(";");
					} else if(newDataType == PkslDataTypes.CONST_INT) {
						expressionBuilder.append("int ").append(input).append(" = ").append(newNoun.getValue()).append(";");
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
	
	public void setExpressions(String lSignature, String rSignature) {
		this.lSignature = lSignature;
		this.rSignature = rSignature;
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
	
	@Override
	public String getJavaSignature() {
		String javaSig = this.signature;
		// replace all the values that is inside this. this could be a recursive call
		for(int i = 0; i < curRow.size(); i++) {
			NounMetadata thisLambdaMeta = curRow.getNoun(i);
			
			Object nextValue = (Object)thisLambdaMeta.getValue();
			
			String replaceValue;
			if(nextValue instanceof JavaExecutable) {
				 replaceValue = ((JavaExecutable)nextValue).getJavaSignature();
			} else {
				continue;
			}
			
			String rSignature;
			if(nextValue instanceof IReactor) {
				rSignature = ((IReactor)nextValue).getSignature();
			} else {
				continue;
			}

			javaSig = modifyJavaSignature(javaSig, rSignature, replaceValue);
		}
		return "0";
//		return javaSig;
	}
}
