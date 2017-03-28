package prerna.sablecc2.om;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewMethod;
import javassist.NotFoundException;

public class Filter {

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

	public boolean evaluate() {
		ClassPool pool = ClassPool.getDefault();
		// generate the method
		StringBuilder method = new StringBuilder();
		
		String lString = getString(lComparison);
		String rString = getString(rComparison);
		
		method.append("public boolean evaluate() {");
		method.append("if(" + lString + this.comparator + rString + ") {"
				+ "return true;"
				+ "} else {"
				+ "return false;"
				+ "}"
				+ "}");
		
		boolean evaluteResult = false;
		String packageName = "t" + System.currentTimeMillis(); // make it unique
		CtClass cc = pool.makeClass(packageName + ".c" + System.currentTimeMillis());
		try {
			cc.setSuperclass(pool.get("prerna.sablecc2.om.FilterEvaluator"));
			cc.addMethod(CtNewMethod.make(method.toString(), cc));
			Class retClass = cc.toClass();
			FilterEvaluator c = (FilterEvaluator) retClass.newInstance();
			evaluteResult = c.evaluate();
		} catch (CannotCompileException e1) {
			e1.printStackTrace();
		} catch (NotFoundException e1) {
			e1.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}

		return evaluteResult;
	}
	
	private String getString(GenRowStruct grs) {
		Object type = grs.get(0);
		PkslDataTypes metaType = grs.getMeta(0);
		if(type instanceof Expression) {
			return ((Expression) type).getExpression();
		} else {
			// if it is a constant
			if(metaType == PkslDataTypes.CONST_STRING) {
				return "\"" + type.toString() + "\"";
			} else {
				return type.toString();
			}
		}
	}
}
