package prerna.sablecc2.om;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javassist.CannotCompileException;
import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.PKSLPlanner;
import prerna.util.Utility;

public class Filter {

	private static final Logger LOGGER = LogManager.getLogger(Filter.class.getName());

	private String comparator = null; //'=', '!=', '<', '<=', '>', '>=', '?like'
	private GenRowStruct lComparison = null; //the column we want to filter
	private GenRowStruct rComparison = null; //the values to bind the filter on
	
	public Filter(GenRowStruct lComparison, String comparator, GenRowStruct rComparison)
	{
		this.lComparison = lComparison;
		this.rComparison = rComparison;
		this.comparator = comparator;
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

	/**
	 * Method to evaluate the filter condition
	 * Only works if all inputs are scalar values (or variables that are scalar values)
	 * @param planner
	 * @return
	 */
	public boolean evaluate(PKSLPlanner planner) {
		ClassPool pool = ClassPool.getDefault();
		
		// string that will contain the new method to execute
		StringBuilder method = new StringBuilder();
		
		NounMetadata lNoun = lComparison.getNoun(0);
		NounMetadata rNoun = rComparison.getNoun(0);
		
		// get the left hand expression
		String lString = getIfExpressionString(planner, lNoun);
		// get the right hand expression
		String rString = getIfExpressionString(planner, rNoun);
		
		// generate the method evaluate
		method.append("public boolean evaluate() {");
//		method.append(variablesToDefineString);
		method.append("if(" + lString + this.comparator + rString + ") {"
				+ "return true;"
				+ "} else {"
				+ "return false;"
				+ "}"
				+ "}");
		
		CtClass cc = pool.makeClass("t" + Utility.getRandomString(12) + ".c" + Utility.getRandomString(12));
		try {
			// class extends FitlerEvaluator
			// it is just an abstract class with the method evaluate above
			// so when we create a new instance
			// we can just cast it and run the method
			cc.setSuperclass(pool.get("prerna.sablecc2.om.FilterEvaluator"));
			cc.addMethod(CtNewMethod.make(method.toString(), cc));
			Class retClass = cc.toClass();
			FilterEvaluator c = (FilterEvaluator) retClass.newInstance();
			boolean evaluteResult = c.evaluate();
			
			// remove the class from the pool
			ClassClassPath ccp = new ClassClassPath(retClass.getClass());
			pool.removeClassPath(ccp);
			
			return evaluteResult;
		} catch (CannotCompileException e1) {
			e1.printStackTrace();
		} catch (NotFoundException e1) {
			e1.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

		// if we got to this point
		// an error occurred in the evaluation of the filter
		throw new IllegalArgumentException("Invalid argument/compilation to evaluate filter (" + lString + " " + comparator + " " + rString + ")");
	}
	
	/**
	 * Method to get the left hand or right hand side of the filter
	 * Currently only handles expressions or constant values
	 * @param grs
	 * @return
	 */
	private String getIfExpressionString(PKSLPlanner planner, NounMetadata noun) {
		Object type = noun.getValue();
		PkslDataTypes metaType = noun.getNounName();
		if(metaType == PkslDataTypes.LAMBDA) {
			// lambda means it is some other reactor (ex. sum) 
			// that is embedded within this filter
			// just execute it and get the value
			
			// if the type is not a string, then it is assumed to be a number
			// so we just return it as a string
			NounMetadata lambdaVal = ((AbstractReactor) type).execute();
			PkslDataTypes lambdaType = ((NounMetadata) lambdaVal).getNounName();
			if(lambdaType == PkslDataTypes.CONST_STRING) {
				return "\"" + lambdaVal.getValue() + "\"";
			} else {
				return lambdaVal.getValue().toString();
			}
		}
		// any other case is a constant
		else if(metaType == PkslDataTypes.CONST_STRING) {
			return "\"" + type.toString() + "\"";
		} 
		// in case it is a column
		// need to check if this is actually a variable
		else if(metaType == PkslDataTypes.COLUMN) {
			if(planner.hasVariable(type.toString())) {
				NounMetadata varNoun = planner.getVariableValue(type.toString());
				// in case the variable itself points to a lambda
				// just re-run this method with the varNoun as input
				return getIfExpressionString(planner, varNoun);
			} else {
				return "\"" + type.toString() + "\"";
			}
		}
		else {
			return type.toString();
		}
	}
}
