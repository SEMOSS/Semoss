package prerna.sablecc2.reactor.planner.graph;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.reactor.BaseJavaRuntime;
import prerna.sablecc2.reactor.ClassMaker;

public class RuntimeJavaClassBuilder {

	List<String> equations;
	List<String> fields;
	Map<String, Object> variables;
	int methodCount;
	private String superClassName;
	private int maxEquationsPerMethod;
	
	public RuntimeJavaClassBuilder() {
		initDefaults();
	}
	
	public void setSuperClass(Class superClass) {
		superClassName = superClass.getName();
	}
	
	private void initDefaults() {
		equations = new ArrayList<>();
		variables = new HashMap<>();
		fields = new ArrayList<>();
		methodCount = 0;
		maxEquationsPerMethod = 1000;
		superClassName = BaseJavaRuntime.class.getName();	
	}
	
	public BaseJavaRuntime buildClass() {
		List<String> methods = buildMethods();
		methods.add(buildMainExecutionMethod());
		return buildRuntimeClass(this.fields, methods);
	}
	
	public void addEquations(List<String> equations) {
		for(String equation : equations) {
			if(!equation.isEmpty()) {
				this.equations.add(equation);
			}
		}
	}
	
	public void addFields(List<String> fields) {
		for(String field : fields) {
			if(!field.isEmpty()) {
				this.fields.add(field);
			}
		}
	}
	
	public void addEquation(String equation) {
		equations.add(equation);
//		String varName = getVarNameFromEquation(equation);
//		addVariable(varName);
	}
	
//	private void addVariable(String var) {
//		variables.put(var, 1);
//	}
//	
//	private void addVariable(String var, Object value) {
//		variables.put(var, value);
//	}
	
	/**
	 * 
	 * @param stringMethods
	 * @return
	 * 
	 */
	private BaseJavaRuntime buildRuntimeClass(List<String> stringMethods) {
		// evaluate the assimilator as an object
		ClassMaker maker = new ClassMaker();

		// add a super so we have a base method to execute
		maker.addSuper(superClassName);
		
		for(String stringMethod : stringMethods) {
			maker.addMethod(stringMethod);
		}
		
		Class newClass = maker.toClass();

		try {
			BaseJavaRuntime newInstance = (BaseJavaRuntime) newClass.newInstance();
			return newInstance;
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}

		return null;
	}
	
	/**
	 * 
	 * @param stringMethods
	 * @return
	 * 
	 */
	private BaseJavaRuntime buildRuntimeClass(List<String> stringFields, List<String> stringMethods) {
		// evaluate the assimilator as an object
		ClassMaker maker = new ClassMaker();

		// add a super so we have a base method to execute
		maker.addSuper(superClassName);
		
		BufferedWriter writer = null;
		FileWriter fw = null;
		try {
			fw = new FileWriter("C:\\Workspace\\Semoss_Dev\\src\\prerna\\sablecc2\\reactor\\test\\ClassTest.txt");
			writer = new BufferedWriter(fw);

			for(String stringField : stringFields) {
				writer.write(stringField);
				writer.write("\n");
			}
			
			for(String stringMethod : stringMethods) {
				writer.write(stringMethod);
				writer.write("\n");
			}
			
			writer.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String stringField : stringFields) {
			maker.addField(stringField);
		}
		
		for(String stringMethod : stringMethods) {
			maker.addMethod(stringMethod);
		}
		
		Class newClass = maker.toClass();

		try {
			BaseJavaRuntime newInstance = (BaseJavaRuntime) newClass.newInstance();
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
		
		curMethod.append("public void execute"+methodCount+"() {"+"\n");
		curMethod.append(varDefs);
		for(String equation : equations) {
			curMethod.append(equation+";");
			String varName = getVarNameFromEquation(equation);
			curMethod.append("addVariable(\""+varName+"\","+varName+");" + "\n");
			
			if(equationCount == maxEquationsPerMethod) {
				curMethod.append("}");
				equationExecutionMethods.add(curMethod.toString());
				
				equationCount = 0;
				methodCount++;
				curMethod = new StringBuilder();
				curMethod.append("public void execute"+methodCount+"() {"+"\n");
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
		method.append("}");
		
		return method.toString();
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
			
			else if(var instanceof String) {
				varDef = "String "+varName+" = \""+var+"\"";
				throw new IllegalArgumentException();
			} else {
				varDef = "String "+varName+" = \""+var.toString()+"\"";
				throw new IllegalArgumentException();
			}
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
				varDef = "addVariable(\""+varName+"\","+((Number)var).intValue()+");";
			} else if(var instanceof String) {
				varDef = "addVariable(\""+varName+"\","+varName+");";
			} else {
				varDef = "addVariable(\""+varName+"\","+varName.toString()+");";
			}
			builder.append(varDef+";");
		}
		return builder;
	}
	
	private String getVarNameFromEquation(String equation) {
		return equation.split("=")[0].trim();
	}
}
