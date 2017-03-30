package prerna.sablecc2.om;

import java.util.HashSet;
import java.util.Set;

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
		
		// get the left hand expression
		String lString = getIfExpressionString(lComparison);
		// get the right hand expression
		String rString = getIfExpressionString(rComparison);
		
		// if variables are used
		// we will create a string and define them at the top of the method
		// this is simpler than having to replace values within the expression
		String variablesToDefineString = getVariablesString(planner, lComparison, rComparison);
		
		// generate the method evaluate
		method.append("public boolean evaluate() {");
		method.append(variablesToDefineString);
		method.append("if(" + lString + this.comparator + rString + ") {"
				+ "return true;"
				+ "} else {"
				+ "return false;"
				+ "}"
				+ "}");
		
		String packageName = "t" + System.currentTimeMillis(); // make it unique
		CtClass cc = pool.makeClass(packageName + ".c" + System.currentTimeMillis());
		
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
	private String getIfExpressionString(GenRowStruct grs) {
		Object type = grs.get(0);
		PkslDataTypes metaType = grs.getMeta(0);
		if(type instanceof Expression) {
			// if the expression depends on a variable
			// that variable is defined in the getVariablesString method
			// and added at the beginning of the class
			return ((Expression) type).getExpression();
		} else if(metaType == PkslDataTypes.LAMBDA) {
			// lambda means it is some other reactor (ex. sum) 
			// that is embedded within this filter
			// just execute it and get the value
			// TODO: need to make sure this is a constant value!
			Object lambdaVal = ((AbstractReactor) type).execute();
			// ideally this is a noun meta data...
			// TODO: currently dont know how to test this piece without other modifications
			// to things like sum... will just keep it like this for now
			if(lambdaVal instanceof NounMetadata) {
				PkslDataTypes lambdaType = ((NounMetadata) lambdaVal).getNounName();
				if(lambdaType == PkslDataTypes.CONST_STRING) {
					return "\"" + type.toString() + "\"";
				} else {
					return type.toString();
				}
			} else {
				LOGGER.error("ENCOUNTERED A LAMBDA WITHIN THE FILTER THAT DIDN'T RETURN NOUN META DATA OBJECT!!!");
				LOGGER.error("ENCOUNTERED A LAMBDA WITHIN THE FILTER THAT DIDN'T RETURN NOUN META DATA OBJECT!!!");
				LOGGER.error("ENCOUNTERED A LAMBDA WITHIN THE FILTER THAT DIDN'T RETURN NOUN META DATA OBJECT!!!");
				LOGGER.error("ENCOUNTERED A LAMBDA WITHIN THE FILTER THAT DIDN'T RETURN NOUN META DATA OBJECT!!!");
				// just going to return the value to string
				// will need to investigate
				return lambdaVal.toString();
			}
		}
		// any other case is a constant
		else if(metaType == PkslDataTypes.CONST_STRING) {
			return "\"" + type.toString() + "\"";
		} else {
			return type.toString();
		}
	}
	
	/**
	 * Method to get the variables to properly evaluate the comparison
	 * @param planner
	 * @param lgrs
	 * @param rgrs
	 * @return
	 */
	private String getVariablesString(PKSLPlanner planner, GenRowStruct lgrs, GenRowStruct rgrs) {
		StringBuilder variablesBuilder = new StringBuilder();
		// need to make sure we do not define the same variable twice
		// so we don't get compilation errors
		Set<String> definedVars = new HashSet<String>();
		
		// right now, we are only taking into consideration
		// expressions... TODO: figure out lambda for everything...
		// also making assumption these inputs lead to constant values
		// cannot handle vectors/iterators
		
		Object ltype = lgrs.get(0);
		varaibleStringGenerator(planner, ltype, variablesBuilder, definedVars);
		Object rtype = rgrs.get(0);
		varaibleStringGenerator(planner, rtype, variablesBuilder, definedVars);

		return variablesBuilder.toString();
	}
	
	public void varaibleStringGenerator(PKSLPlanner planner, Object genRowValue, StringBuilder variablesBuilder, Set<String> definedVars) {
		// currently only handling the case of columns defined within expressions
		// not sure if there is any other case to look into
		if(genRowValue instanceof Expression) {
			// grab the inputs
			String[] inputs = ((Expression) genRowValue).getInputs();
			for(String input : inputs) {
				// only append new inputs that haven't already been defined
				// the java method will not compile if defining the same variable twice
				if(definedVars.contains(input)) {
					continue;
				}
				// based on type of variable
				// make sure to define it correctly
				// currently only handling doubles and strings
				// ... not sure how a string will be used yet...
				NounMetadata data = planner.getVariable(input);
				if(data.getNounName() == PkslDataTypes.CONST_DECIMAL) {
					variablesBuilder.append("double ").append(input).append(" = ").append(data.getValue()).append(";");
				} else if(data.getNounName() == PkslDataTypes.CONST_STRING) {
					variablesBuilder.append("String ").append(input).append(" = \"").append(data.getValue()).append("\";");
				} else {
					throw new IllegalArgumentException("Filter can only handle scalar variables");
				}
				
				// after defining the variable
				// add it to the set so we do not define it again
				definedVars.add(input);
			}
		}
	}
}
