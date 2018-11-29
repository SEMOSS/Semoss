package prerna.sablecc2.reactor;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.codehaus.plexus.util.StringUtils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class Assimilator extends AbstractReactor implements JavaExecutable {

	// roles of the assimilator is simple, just assimilate an expression and then
	// plug it into the parent
	// filter is a good example of assimilator for example
	private boolean containsStringValue = false;

	@Override
	public NounMetadata execute() {
		modifySignatureFromLambdas();
		
		// get the assimilator evaluator
		// this is the class we are going to be using to execute
		// if we are running this again, we will not create the class and add
		// it to the ClassPool, but if it is new, we will
		AssimilatorEvaluator newInstance = getAssimilatorEvaluator();
		// set the values into the new instance's var map
		// this is implemented this way so we can re-use the class
		// even if a few variables are changed
		setInstanceVariables(newInstance);

		// noun object to return
		// need to cast to get the type of the NounMetadata object
		NounMetadata noun = new NounMetadata(newInstance, PixelDataType.LAMBDA);
		return noun;
		
//		Object retVal = newInstance.execute();
//		if(newInstance.containsStringValue) {
//			noun = new NounMetadata(retVal.toString(), PkslDataTypes.CONST_STRING);
//		} else if(allIntValue) {
//			Number result = (Number) retVal;
//			if(result.doubleValue() == Math.rint(result.doubleValue())) {
//				noun = new NounMetadata( ((Number) retVal).intValue(), PkslDataTypes.CONST_INT);
//			} else {
//				// not a valid integer
//				// return as a double
//				noun = new NounMetadata( ((Number) retVal).doubleValue(), PkslDataTypes.CONST_DECIMAL);
//			}
//		} else {
//			noun = new NounMetadata( ((Number) retVal).doubleValue(), PkslDataTypes.CONST_DECIMAL);
//		}
//
//		return noun;
	}

	/**
	 * 
	 * @param evaluator
	 * 
	 * This method sets the values to the evaluator through the abstract
	 */
	private void setInstanceVariables(AssimilatorEvaluator evaluator) {
		List<String> inputColumns = curRow.getAllColumns();
		// these input columns should be defined at the beginning of the expression
		// technically, someone can use the same variable multiple times
		// so need to account for this
		// ... just add them to a set and call it a day
		Set<String> uniqueInputs = new HashSet<String>();
		uniqueInputs.addAll(inputColumns);
		for(String input : uniqueInputs) {
			NounMetadata data = planner.getVariableValue(input);			
			PixelDataType dataType = data.getNounType();
			if(dataType == PixelDataType.CONST_DECIMAL) {
				evaluator.allIntValue = false;
				evaluator.setVar(input, data.getValue());
			} else if(dataType == PixelDataType.CONST_INT) {
				evaluator.setVar(input, data.getValue());
			} else if(dataType == PixelDataType.CONST_STRING) {
				evaluator.containsStringValue = true;
				evaluator.setVar(input, data.getValue());
			} else if(dataType == PixelDataType.LAMBDA){
				// in case the variable points to another reactor
				// that we need to get the value from
				// evaluate the lambda
				// object better be a reactor to run
				Object rVal = data.getValue();
				if(rVal instanceof IReactor) {
					NounMetadata newNoun = ((IReactor) rVal).execute(); 
					PixelDataType newDataType = data.getNounType();
					if(newDataType == PixelDataType.CONST_DECIMAL) {
						evaluator.setVar(input, newNoun.getValue());
					} else if(newDataType == PixelDataType.CONST_STRING) {
						evaluator.containsStringValue = true;
						evaluator.setVar(input, newNoun.getValue());
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

	/**
	 * 
	 * method to get the run time class for the assimilator
	 */
	private AssimilatorEvaluator getAssimilatorEvaluator() {
		AssimilatorEvaluator evaluator;

		//stringified method for the evaluator
		String stringMethod = buildMethodString();
		//id for this particular assimilator
		String classId = "$Assimilator"+stringMethod.hashCode();
		//if we have an existing one, grab that
		if(this.planner.hasVariable(classId)) {
			evaluator = (AssimilatorEvaluator)this.planner.getVariable(classId).getValue();
		} 

		//otherwise build a new one
		else {
			evaluator = buildAssimilatorEvaluator(stringMethod);
//			NounMetadata newEvaluator = new NounMetadata(evaluator, PkslDataTypes.CACHED_CLASS);
//			this.planner.addVariable(classId, newEvaluator);
		}

		return evaluator;
	}

	/**
	 * 
	 * @param stringMethod
	 * @return
	 * 
	 * method responsible for building a new assimilator class from a stringified method
	 */
	private AssimilatorEvaluator buildAssimilatorEvaluator(String stringMethod) {
		// evaluate the assimilator as an object
		ClassMaker maker = new ClassMaker();
		// add a super so we have a base method to execute
		maker.addSuper("prerna.sablecc2.reactor.AssimilatorEvaluator");
		maker.addMethod(stringMethod);
		Class newClass = maker.toClass();

		try {
			AssimilatorEvaluator newInstance = (AssimilatorEvaluator) newClass.newInstance();
			newInstance.containsStringValue = this.containsStringValue;
			return newInstance;
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * method responsible for building the method
	 */
	private String buildMethodString() {
		// keep a string to generate the method to execute that will
		// return an object that runs the expression
		StringBuilder expressionBuilder = new StringBuilder();
		expressionBuilder.append("public Object getExpressionValue(){");
		// we need to grab any variables and define them at the top of the method
		appendVariables(expressionBuilder);
		// now that the variables are defined
		// we just want to add the expression as a return
		if(this.containsStringValue) {
			expressionBuilder.append("return new String(").append(this.signature).append(");}");
		} else {
			// multiply by 1.0 to make sure everything is a double...
			// as a pixel data type
			expressionBuilder.append("return new java.math.BigDecimal(1.0 * ( ").append(this.signature).append("));}");
		}
		return expressionBuilder.toString();
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
			PixelDataType dataType = data.getNounType();
			if(dataType == PixelDataType.CONST_DECIMAL) {
				expressionBuilder.append("double ").append(input).append(" = ").append("((Number)super.vars.get("+"\""+input+"\")).doubleValue()").append(";");
			} else if(dataType == PixelDataType.CONST_INT) {
				expressionBuilder.append("int ").append(input).append(" = ").append("((Number)super.vars.get("+"\""+input+"\")).intValue()").append(";");
			} else if(dataType == PixelDataType.CONST_STRING) {
				this.containsStringValue = true;
				expressionBuilder.append("String ").append(input).append(" = ").append("(String)super.vars.get("+"\""+input+"\")").append(";");
			} else if(dataType == PixelDataType.LAMBDA){
				// in case the variable points to another reactor
				// that we need to get the value from
				// evaluate the lambda
				// object better be a reactor to run
				Object rVal = data.getValue();
				if(rVal instanceof IReactor) {
					PixelDataType newDataType = data.getNounType();
					if(newDataType == PixelDataType.CONST_DECIMAL) {
						expressionBuilder.append("double ").append(input).append(" = ").append("((Number)super.vars.get("+"\""+input+"\")).doubleValue()").append(";");
					} else if(newDataType == PixelDataType.CONST_INT) {
						expressionBuilder.append("int ").append(input).append(" = ").append("((Number)super.vars.get("+"\""+input+"\")).intValue()").append(";");
					} else if(newDataType == PixelDataType.CONST_STRING) {
						this.containsStringValue = true;
						expressionBuilder.append("String ").append(input).append(" = ").append("(String)super.vars.get("+"\""+input+"\")").append(";");
					}
				} else {
					// this should never ever happen....
					throw new IllegalArgumentException("Assimilator cannot handle this type if input");
				}
			} else {
				throw new IllegalArgumentException("Assimilator can currently only handle outputs of scalar variables");
			}
		}
		
		// need to how check if there are strings
		int numValues = this.curRow.size();
		for(int i = 0; i < numValues; i++) {
			if(this.curRow.getNoun(i).getNounType() == PixelDataType.CONST_STRING) {
				this.containsStringValue = true;
				break;
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
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.LAMBDA);
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
				rSignature = ((IReactor)nextValue).getOriginalSignature();
			} else {
				continue;
			}
//			NounMetadata result = thisReactor.execute();// this might further trigger other things

//			// for compilation reasons
//			// if we have a double
//			// we dont want it to print with the exponential
//			Object replaceValue = result.getValue();
//			PkslDataTypes replaceType = result.getNounName();
//			if(replaceType == PkslDataTypes.CONST_DECIMAL || 
//					replaceType == PkslDataTypes.CONST_INT) {
//				// big decimal is easiest way i have seen to do this formatting
//				replaceValue = new BigDecimal( ((Number) replaceValue).doubleValue()).toPlainString();
//			} else {
//				replaceValue = replaceValue + "";
//			}
			javaSig = modifyJavaSignature(javaSig, rSignature, replaceValue);
		}
		
		return javaSig;
//		return null;
	}
	
	public String modifyJavaSignature(String javaSignature, String stringToFind, String stringReplacement) {
		return StringUtils.replaceOnce(javaSignature, stringToFind, stringReplacement);
	}

	@Override
	public List<NounMetadata> getJavaInputs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getReturnType() {
		return "double";
	}
}
