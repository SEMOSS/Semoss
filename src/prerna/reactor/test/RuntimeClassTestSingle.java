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

public class RuntimeClassTestSingle extends AbstractReactor {

	List<String> equations = new ArrayList<>();
	Map<String, Object> variables = new HashMap<>();

	public static void main(String[] args) {
		
		String[] equations = getEquations();
		
		long startTime = System.currentTimeMillis();
		for(String equation : equations) {
			RuntimeClassTestSingle test = new RuntimeClassTestSingle();
			addVariables(test);
			test.equations.add(equation);
			AbstractTestClass testClass = test.buildTestClass(test.buildMethod());
			testClass.execute();
//			Map<String, Object> varMap = testClass.getVariables();
//			for(String var : varMap.keySet()) {
//				System.out.println(var+" : "+varMap.get(var));
//			}
		}
		long endTime = System.currentTimeMillis();
		System.out.println(endTime - startTime);
	}
	
	@Override
	public NounMetadata execute() {
		AbstractTestClass testClass = buildTestClass(buildMethod());
		testClass.execute();
		Map<String, Object> vars = testClass.getVariables();
		return null;
	}
	
	private void addEquation(String equation) {
		equations.add(equation);
		String varName = getVarNameFromEquation(equation);
		addVariable(varName);
	}
	
	private void addVariable(String var) {
		variables.put(var, 1.0);
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
	private AbstractTestClass buildTestClass(String stringMethod) {
		// evaluate the assimilator as an object
		ClassMaker maker = new ClassMaker();

		// add a super so we have a base method to execute
		maker.addSuper("prerna.reactor.test.AbstractTestClass");
		maker.addMethod(stringMethod);
		Class newClass = maker.toClass();

		try {
			AbstractTestClass newInstance = (AbstractTestClass) newClass.newInstance();
			return newInstance;
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}

		return null;
	}
	
	private String buildMethod() {
		StringBuilder method = new StringBuilder();
		method.append("public void execute() {");
		method.append(getVarDefinitions());
		method.append(getEquationDefinitions());
		method.append("}");
		
		return method.toString();
	}
	
	private PixelPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.PLANNER.toString());
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
			String varDef;
			if(var instanceof Number) {
				varDef = "int "+varName+" = "+((Number)var).intValue();
			} else if(var instanceof String) {
				varDef = "String "+varName+" = \""+var+"\"";
			} else {
				varDef = "String "+varName+" = \""+var.toString()+"\"";
			}
			builder.append(varDef+";");
		}
		return builder;
	}
	
	private String getVarNameFromEquation(String equation) {
		return equation.split("=")[0].trim();
	}
	
	private static void addVariables(RuntimeClassTestSingle test) {
		Random rand = new Random();
		for(int i = 0; i < 26; i++) {
			String varName = ((char)('a'+i))+"";
			test.addVariable(varName, rand.nextInt() + 1);
		}
	}
	
	private static String[] getEquations() {
		EquationGenerator eg = new EquationGenerator();
		String[] equations = eg.getRandomEquations(100000);
		return equations;
	}
}
