package prerna.sablecc2.om;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import prerna.reactor.AbstractReactor;
import prerna.reactor.JavaExecutable;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class Filter implements JavaExecutable {

	private static final Logger classLogger = LogManager.getLogger(Filter.class);

	private String comparator = null; //'=', '!=', '<', '<=', '>', '>=', '?like'
	private GenRowStruct lComparison = null; //the column we want to filter
	private GenRowStruct rComparison = null; //the values to bind the filter on
	private VarStore varStore = null;
	
	public Filter(GenRowStruct lComparison, String comparator, GenRowStruct rComparison)
	{
		this.lComparison = lComparison;
		this.rComparison = rComparison;
		if("<>".equals(comparator)) {
			this.comparator = "!=";
		} else {
			this.comparator = comparator;
		}
	}

	public GenRowStruct getLComparison() {
		return lComparison;
	}
	
	public GenRowStruct getRComparison() {
		return rComparison;
	}
	
	public String getComparator() {
		return this.comparator;
	}

	public void setVarStore(VarStore varStore) {
		this.varStore = varStore;
	}
	
	/**
	 * Method to evaluate the filter condition
	 * Only works if all inputs are scalar values (or variables that are scalar values)
	 * @param planner
	 * @return
	 */
	public boolean evaluate() {
		FilterEvaluator c = getFilterEvaluator();
		//set left var
		//set right var
		NounMetadata lNoun = lComparison.getNoun(0);
		setIfExpression(c, lNoun, "LEFT");
		NounMetadata rNoun = rComparison.getNoun(0);
		setIfExpression(c, rNoun, "RIGHT");
		boolean evaluateResult = c.evaluate();
		return evaluateResult;
	}
	
	/**
	 * 
	 * @return
	 * 
	 * Creates new and stores, OR retrieves existing filter evaluator class we need
	 */
	private FilterEvaluator getFilterEvaluator() {
		FilterEvaluator evaluator;
		
		//the method string
		String stringMethod = buildMethodString();
		
		//the id associated with this filter evaluator
		String classId = "$Filter"+stringMethod.hashCode();
		
		//if exists, use it
		if(this.varStore.containsKey(classId)) {
			evaluator = (FilterEvaluator)this.varStore.get(classId).getValue();
		} 
		
		//else create a new one, store it, return
		else {
			evaluator = buildFilterEvaluator(stringMethod);
//			NounMetadata newEvaluator = new NounMetadata(evaluator, PixelDataTypes.CACHED_CLASS);
//			this.planner.addVariable(classId, newEvaluator);
		}
		
		return evaluator;
	}
	
	/**
	 * 
	 * @param stringMethod
	 * @return
	 * 
	 * This method is responsible for creating a new filter evaluator from a stringified method
	 */
	private FilterEvaluator buildFilterEvaluator(String stringMethod) {
		ClassPool pool = ClassPool.getDefault();
		
		CtClass cc = pool.makeClass("t" + Utility.getRandomString(12) + ".c" + Utility.getRandomString(12));
		try {
			// class extends FitlerEvaluator
			// it is just an abstract class with the method evaluate above
			// so when we create a new instance
			// we can just cast it and run the method
			cc.setSuperclass(pool.get("prerna.sablecc2.om.FilterEvaluator"));
			cc.addMethod(CtNewMethod.make(stringMethod, cc));
			Class retClass = cc.toClass();
			FilterEvaluator c = (FilterEvaluator) retClass.newInstance();
			return c;
		} catch (CannotCompileException e1) {
			e1.printStackTrace();
		} catch (NotFoundException e1) {
			e1.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	/**
	 * 
	 * @return
	 * 
	 * This method builds the stringified method we will need for the filter evaluator
	 */
	private String buildMethodString() {
		
		// string that will contain the new method to execute
		StringBuilder method = new StringBuilder();
		
		NounMetadata lNoun = lComparison.getNoun(0);
		NounMetadata rNoun = rComparison.getNoun(0);
		
		// get the left hand expression
//		String lString = getIfExpressionString(planner, lNoun);
		String lString = getIfExpressionString(lNoun, "LEFT");
		// get the right hand expression
//		String rString = getIfExpressionString(planner, rNoun);
		String rString = getIfExpressionString(rNoun, "RIGHT");
		
		// generate the method evaluate
		method.append("public boolean evaluate() {");
//		method.append(variablesToDefineString);
		method.append("if(" + lString + this.comparator + rString + ") {"
				+ "return true;"
				+ "} else {"
				+ "return false;"
				+ "}"
				+ "}");
		
		return method.toString();
	}
	
	/**
	 * Method to get the left hand or right hand side of the filter
	 * Currently only handles expressions or constant values
	 * 
	 * key is 'LEFT' or 'RIGHT'
	 * @param grs
	 * @return
	 */
	private String getIfExpressionString(NounMetadata noun, String key) {
		Object type = noun.getValue();
		PixelDataType metaType = noun.getNounType();
		if(metaType == PixelDataType.LAMBDA) {
			// lambda means it is some other reactor (ex. sum) 
			// that is embedded within this filter
			// just execute it and get the value
			
			// if the type is not a string, then it is assumed to be a number
			// so we just return it as a string
			NounMetadata lambdaVal = ((AbstractReactor) type).execute();
			PixelDataType lambdaType = ((NounMetadata) lambdaVal).getNounType();
			if(lambdaType == PixelDataType.CONST_STRING) {
//				return "\"" + lambdaVal.getValue() + "\"";
				return getStringExpression(key);
			} else {
				return getDoubleExpression(key);
//				return lambdaVal.getValue().toString();
			}
		}
		// any other case is a constant
		else if(metaType == PixelDataType.CONST_STRING) {
//			return "\"" + type.toString() + "\"";
			return getStringExpression(key);
		} 
		// in case it is a column
		// need to check if this is actually a variable
		else if(metaType == PixelDataType.COLUMN) {
			if(this.varStore.containsKey(type.toString())) {
				NounMetadata varNoun = this.varStore.getEvaluatedValue(type.toString());
				// in case the variable itself points to a lambda
				// just re-run this method with the varNoun as input
				return getIfExpressionString(varNoun, key);
			} else {
//				return "\"" + type.toString() + "\"";
				return getStringExpression(key);
			}
		}
		else {
			//TODO: this could also be a boolean?
			return getDoubleExpression(key);
		}
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 * 
	 * Stringified code bit to get a string value from the abstract
	 */
	public String getStringExpression(String key) {
		return "(String)super.vars.get(\""+key+"\")";
	}
	
	/**
	 * 
	 * @param key
	 * @return
	 * 
	 * Stringified code bit to get a double value from the abstract
	 */
	public String getDoubleExpression(String key) {
		return "((Number)super.vars.get(\""+key+"\")).doubleValue()";
	}
	/**
	 * 
	 * @param grs
	 * @return
	 * 
	 * This method sets the values to the evaluator (specifically through the abstract)
	 */
	private void setIfExpression(FilterEvaluator evaluator, NounMetadata noun, String key) {
		Object type = noun.getValue();
		PixelDataType metaType = noun.getNounType();
		if(metaType == PixelDataType.LAMBDA) {
			// lambda means it is some other reactor (ex. sum) 
			// that is embedded within this filter
			// just execute it and get the value
			
			// if the type is not a string, then it is assumed to be a number
			// so we just return it as a string
			NounMetadata lambdaVal = ((AbstractReactor) type).execute();
			PixelDataType lambdaType = ((NounMetadata) lambdaVal).getNounType();
			if(lambdaType == PixelDataType.CONST_STRING) {
				evaluator.setVar(key, lambdaVal.getValue().toString());
			} else {
				evaluator.setVar(key, lambdaVal.getValue());
			}
		}
		// any other case is a constant
		else if(metaType == PixelDataType.CONST_STRING) {
			evaluator.setVar(key, type.toString());
		} 
		// in case it is a column
		// need to check if this is actually a variable
		else if(metaType == PixelDataType.COLUMN) {
			if(this.varStore.containsKey(type.toString())) {
				NounMetadata varNoun = this.varStore.getEvaluatedValue(type.toString());
				// in case the variable itself points to a lambda
				// just re-run this method with the varNoun as input
				setIfExpression(evaluator, varNoun, key);
			} else {
				evaluator.setVar(key, type.toString());
			}
		}
		else {
			evaluator.setVar(key, type);
		}
	}

	@Override
	public String getJavaSignature() {
		Object left = lComparison.get(0);
		Object right = rComparison.get(0);
		
		String leftSide;
		String rightSide;
		
		if(left instanceof JavaExecutable) {
			leftSide = ((JavaExecutable)left).getJavaSignature();
		} else {
			leftSide = left.toString();
		}
		
		if(right instanceof JavaExecutable) {
			rightSide = ((JavaExecutable)right).getJavaSignature();
		} else {
			rightSide = right.toString();
		}
		
		return leftSide + this.comparator + rightSide;
	}

	@Override
	public List<NounMetadata> getJavaInputs() {
		return null;
	}

	@Override
	public String getReturnType() {
		// TODO Auto-generated method stub
		return "boolean";
	}
}
