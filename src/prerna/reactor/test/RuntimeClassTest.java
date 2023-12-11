package prerna.reactor.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import prerna.reactor.AbstractReactor;
import prerna.reactor.ClassMaker;
import prerna.reactor.PixelPlanner;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RuntimeClassTest extends AbstractReactor {

	List<String> equations = new ArrayList<>();
	Map<String, Object> variables = new HashMap<>();
	int methodCount = 0;
	
//	public static void main(String[] args) {
//		long startTime = System.currentTimeMillis();
//		RuntimeClassTest test = new RuntimeClassTest();
//		addVariables(test);
//		addEquations(test);
//		List<String> methods  = test.buildMethods();
//		methods.add(test.buildMainExecutionMethod());
//		AbstractTestClass testClass = test.buildTestClass(methods);
//		testClass.execute();
//		
//		long endTime = System.currentTimeMillis();
//		System.out.println(endTime - startTime);
//		Map<String, Object> varMap = testClass.getVariables();
//		for(String var : varMap.keySet()) {
//			System.out.println(var+" : "+varMap.get(var));
//		}
//	}
	
	@Override
	public NounMetadata execute() {
		
//		AbstractTestClass testClass = buildTestClass(buildMethod());
//		testClass.execute();
//		Map<String, Object> vars = testClass.getVariables();
		return null;
	}
	
	private void addEquation(String equation) {
		equations.add(equation);
		String varName = getVarNameFromEquation(equation);
		addVariable(varName);
	}
	
	private void addVariable(String var) {
		variables.put(var, 1);
	}
	
	private void addVariable(String var, Object value) {
		variables.put(var, value);
	}
	
	/**
	 * 
	 * @param stringMethod
	 * @return
	 * 
	 * method responsible for building a new assimilator class from a stringified method
	 */
	private AbstractTestClass buildTestClass(List<String> stringMethods) {
		// evaluate the assimilator as an object
		ClassMaker maker = new ClassMaker();

		// add a super so we have a base method to execute
		maker.addSuper(AbstractTestClass.class.getName());
		for(String stringMethod : stringMethods) {
			maker.addMethod(stringMethod);
		}
		Class newClass = maker.toClass();

		try {
			AbstractTestClass newInstance = (AbstractTestClass) newClass.newInstance();
			return newInstance;
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}

		return null;
	}
	
	private List<String> buildMethods() {
		List<String> equationExecutionMethods = new ArrayList<>();
		int equationCount = 0;
		methodCount = 1;
		StringBuilder curMethod = new StringBuilder();
		String varDefs = getVarDefinitions().toString();
		
		curMethod.append("public void execute"+methodCount+"() {");
		curMethod.append(varDefs);
		for(String equation : equations) {
			curMethod.append(equation+";");
			String varName = getVarNameFromEquation(equation);
			curMethod.append("addVariable(\""+varName+"\","+varName+");");
			
			if(equationCount == 2000) {
				curMethod.append("}");
				equationExecutionMethods.add(curMethod.toString());
				
				equationCount = 0;
				methodCount++;
				curMethod = new StringBuilder();
				curMethod.append("public void execute"+methodCount+"() {");
				curMethod.append(varDefs);
			} else {
				equationCount++;
			}
		}
		curMethod.append("}");
		equationExecutionMethods.add(curMethod.toString());
		return equationExecutionMethods;
	}
	
	private String buildMainExecutionMethod() {
		StringBuilder method = new StringBuilder();
		method.append("public void execute() {");
		method.append(getInitVarSettings());
		for(int i = 1; i <= methodCount; i++) {
			method.append("execute"+i+"();");
		}
//		method.append(getEquationDefinitions());
		method.append("}");
		
		return method.toString();
	}
	
	private PixelPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.PLANNER.getKey());
		PixelPlanner planner = null;
		if(allNouns != null) {
			planner = (PixelPlanner) allNouns.get(0);
			return planner;
		} else {
			return this.planner;
		}
	}
	
	private StringBuilder getEquationDefinitions() {
		StringBuilder equationDefs = new StringBuilder();
		for(String equation : equations) {
			equationDefs.append(equation+";");
			String varName = getVarNameFromEquation(equation);
			equationDefs.append("addVariable(\""+varName+"\","+varName+");");
		}
		return equationDefs;
	}
	
	private StringBuilder getVarDefinitions() {
		StringBuilder builder = new StringBuilder();
		for(String varName : variables.keySet()) {
			Object var = variables.get(varName);
			String varDef = "";
			if(var instanceof Number) {
				varDef = "int "+varName+" = ((Number)getVariable(\""+varName+"\")).intValue()";
			} 
			
//			else if(var instanceof String) {
//				varDef = "String "+varName+" = \""+var+"\"";
//				throw new IllegalArgumentException();
//			} else {
//				varDef = "String "+varName+" = \""+var.toString()+"\"";
//				throw new IllegalArgumentException();
//			}
			builder.append(varDef+";");
		}
		return builder;
	}
	
	private StringBuilder getInitVarSettings() {
		StringBuilder builder = new StringBuilder();
		for(String varName : variables.keySet()) {
			Object var = variables.get(varName);
			String varDef = "";
			if(var instanceof Number) {
//				varDef = "int "+varName+" = "+((Number)var).intValue();
				varDef = "addVariable(\""+varName+"\","+((Number)var).intValue()+");";
			} else if(var instanceof String) {
//				varDef = "String "+varName+" = \""+var+"\"";
//				throw new IllegalArgumentException();
			} else {
//				varDef = "String "+varName+" = \""+var.toString()+"\"";
//				throw new IllegalArgumentException();
			}
			builder.append(varDef+";");
		}
		return builder;
	}
	
	private String getVarNameFromEquation(String equation) {
		return equation.split("=")[0].trim();
	}
	
	private static void addVariables(RuntimeClassTest test) {
		Random rand = new Random();
		for(int i = 0; i < 26; i++) {
			String varName = ((char)('a'+i))+"";
			test.addVariable(varName, rand.nextInt(100)+1);
		}
	}
	
	private static void addEquations(RuntimeClassTest test) {
		EquationGenerator eg = new EquationGenerator();
		
		String[] equations = eg.getRandomEquations(100000);		
		for(String equation : equations) {
			test.addEquation(equation);
		}
	}
}
