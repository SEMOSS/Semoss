package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.codehaus.plexus.util.StringUtils;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskUtility;

public class Assimilator extends AbstractReactor implements JavaExecutable {

	// roles of the assimilator is simple, just assimilate an expression and then
	// plug it into the parent
	// filter is a good example of assimilator for example
	private boolean containsStringValue = false;

	protected List<String> formulas = new Vector<String>();
	
	@Override
	public NounMetadata execute() {
		modifySignatureFromLambdas();
		for(String formula : formulas) {
			this.signature = StringUtils.replaceOnce( this.signature, formula, "( 1.0 * " + formula.substring(1, formula.length()));
		}
		// get the assimilator evaluator
		// this is the class we are going to be using to execute
		// if we are running this again, we will not create the class and add
		// it to the ClassPool, but if it is new, we will
		AssimilatorEvaluator newInstance = getAssimilatorEvaluator();

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
	 * method to get the run time class for the assimilator
	 */
	private AssimilatorEvaluator getAssimilatorEvaluator() {
		// store the variables
		// this will get added to by reference
		Map<String, Object> vars = new HashMap<String, Object>();
		//stringified method for the evaluator
		String stringMethod = buildMethodString(vars);
		return buildAssimilatorEvaluator(stringMethod, vars);
	}

	/**
	 * 
	 * @param stringMethod
	 * @return
	 * 
	 * method responsible for building a new assimilator class from a stringified method
	 */
	private AssimilatorEvaluator buildAssimilatorEvaluator(String stringMethod, Map<String, Object> vars) {
		// evaluate the assimilator as an object
		ClassMaker maker = new ClassMaker();
		// add a super so we have a base method to execute
		maker.addSuper("prerna.sablecc2.reactor.AssimilatorEvaluator");
		maker.addMethod(stringMethod);
		Class newClass = maker.toClass();

		try {
			AssimilatorEvaluator newInstance = (AssimilatorEvaluator) newClass.newInstance();
			newInstance.setVars(vars);
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
	private String buildMethodString(Map<String, Object> vars) {
		// keep a string to generate the method to execute that will
		// return an object that runs the expression
		StringBuilder expressionBuilder = new StringBuilder();
		expressionBuilder.append("public Object getExpressionValue(){");
		// we need to grab any variables and define them at the top of the method
		appendVariables(expressionBuilder, vars);
		// now that the variables are defined
		// we just want to add the expression as a return
		if(this.containsStringValue) {
			expressionBuilder.append("return new String(").append(this.signature).append(");}");
		} else {
			// multiply by 1.0 to make sure everything is a double...
			// as a pixel data type
			expressionBuilder.append("return new java.math.BigDecimal(1.0 * ").append(this.signature).append(");}");
		}
		return expressionBuilder.toString();
	}

	/**
	 * Append the variables used within the expression
	 * @param expressionBuilder
	 */
	private void appendVariables(StringBuilder expressionBuilder, Map<String, Object> vars) {
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
			Object value = data.getValue();
			PixelDataType dataType = data.getNounType();
			if(dataType == PixelDataType.CONST_DECIMAL) {
				expressionBuilder.append("double ").append(input).append(" = ").append("((Number)super.vars.get("+"\""+input+"\")).doubleValue()").append(";");
				vars.put(input, value);
			} else if(dataType == PixelDataType.CONST_INT) {
				expressionBuilder.append("int ").append(input).append(" = ").append("((Number)super.vars.get("+"\""+input+"\")).intValue()").append(";");
				vars.put(input, value);
			} else if(dataType == PixelDataType.CONST_STRING) {
				this.containsStringValue = true;
				expressionBuilder.append("String ").append(input).append(" = ").append("(String)super.vars.get("+"\""+input+"\")").append(";");
				vars.put(input, value);
			} else if(dataType == PixelDataType.LAMBDA){
				// in case the variable points to another reactor
				// that we need to get the value from
				// evaluate the lambda
				// object better be a reactor to run
				Object rVal = data.getValue();
				if(rVal instanceof IReactor) {
					Object newValue = data.getValue();
					PixelDataType newDataType = data.getNounType();
					if(newDataType == PixelDataType.CONST_DECIMAL) {
						expressionBuilder.append("double ").append(input).append(" = ").append("((Number)super.vars.get("+"\""+input+"\")).doubleValue()").append(";");
						vars.put(input, newValue);
					} else if(newDataType == PixelDataType.CONST_INT) {
						expressionBuilder.append("int ").append(input).append(" = ").append("((Number)super.vars.get("+"\""+input+"\")).intValue()").append(";");
						vars.put(input, newValue);
					} else {
						this.containsStringValue = true;
						expressionBuilder.append("String ").append(input).append(" = ").append("(String)super.vars.get("+"\""+input+"\")").append(";");
						vars.put(input, newValue);
					}
				} else {
					// this should never ever happen....
					throw new IllegalArgumentException("Assimilator cannot handle this type if input");
				}
			} else if(dataType == PixelDataType.FORMATTED_DATA_SET) {
				NounMetadata formatData = TaskUtility.getTaskDataScalarElement(value);
				if(formatData == null) {
					throw new IllegalArgumentException("Can only handle query data that is a scalar input");
				} else {
					Object newValue = formatData.getValue();
					PixelDataType newDataType = formatData.getNounType();
					if(newDataType == PixelDataType.CONST_DECIMAL) {
						expressionBuilder.append("double ").append(input).append(" = ").append("((Number)super.vars.get("+"\""+input+"\")).doubleValue()").append(";");
						vars.put(input, newValue);
					} else if(newDataType == PixelDataType.CONST_INT) {
						expressionBuilder.append("int ").append(input).append(" = ").append("((Number)super.vars.get("+"\""+input+"\")).intValue()").append(";");
						vars.put(input, newValue);
					} else {
						this.containsStringValue = true;
						expressionBuilder.append("String ").append(input).append(" = ").append("(String)super.vars.get("+"\""+input+"\")").append(";");
						vars.put(input, newValue);
					}
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
	
	public void addFormula(String formula) {
		// always append at the beginning
		// so that we address the most inner one
		this.formulas.add(0, formula);
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
	}
	
	protected String modifyJavaSignature(String javaSignature, String stringToFind, String stringReplacement) {
		return StringUtils.replaceOnce(javaSignature, stringToFind, stringReplacement);
	}

	@Override
	public List<NounMetadata> getJavaInputs() {
		return null;
	}

	@Override
	public String getReturnType() {
		return "double";
	}
}
