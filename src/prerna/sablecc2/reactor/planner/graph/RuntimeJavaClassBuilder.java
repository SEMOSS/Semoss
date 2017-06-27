package prerna.sablecc2.reactor.planner.graph;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.storm.shade.com.google.common.collect.Lists;

import prerna.sablecc2.reactor.BaseJavaRuntime;
import prerna.sablecc2.reactor.ClassMaker;

public class RuntimeJavaClassBuilder {

	List<String> equations;
	List<String> fields;
	Set<String> variables;
	int methodCount;
	private String superClassName;
	private int maxEquationsPerMethod;
	private int maxFieldsPerClass;
	private int maxMethodsPerClass;
	private int superClassesCount;

	public RuntimeJavaClassBuilder() {
		initDefaults();
	}

	public void setSuperClass(Class superClass) {
		superClassName = superClass.getName();
	}

	private void initDefaults() {
		equations = new ArrayList<>();
		variables = new HashSet<String>();
		fields = new ArrayList<>();
		methodCount = 0;
		maxEquationsPerMethod = 700;
		maxFieldsPerClass = 7000;
		maxMethodsPerClass = 30;
		superClassName = BaseJavaRuntime.class.getName();
	}

	public BaseJavaRuntime buildClass() {
		List<String> methods = buildInitMethods();
		methods.addAll(buildMethods());
		methods.add(buildMainExecutionMethod());
		return buildRuntimeClass(this.fields, methods);
	}

	public BaseJavaRuntime buildExtendedClass() {
		List<String> methods = buildInitMethods();
		methods.addAll(buildMethods());
		return buildRuntimeClassSeperately(this.fields, methods);
	}

	public BaseJavaRuntime buildSimpleClass() {
		List<String> methods = new ArrayList<String>();
		methods.add(buildMainExecutionMethod());
		return buildRuntimeClass(this.fields, methods);
	}

	public BaseJavaRuntime buildUpdateClass() {
		List<String> methods = new ArrayList<String>();
		methods.add(buildUpdateMethods());
		methods.add(buildMainExecutionMethod());
		return buildRuntimeClass(this.fields, methods);
	}

	public void addEquations(List<String> equations) {
		for (String equation : equations) {
			if (!equations.isEmpty()) {
				this.equations.add(equation);
			}
		}
	}

	public void addFields(List<String> fields) {
		for (String field : fields) {
			if (!field.isEmpty()) {
				this.fields.add(field);
			}
		}
	}

	public void addEquation(String equation) {
		equations.add(equation);
	}

	private BaseJavaRuntime buildRuntimeClassSeperately(List<String> stringFields, List<String> stringMethods) {
		buildSuperClassWithOnlyFields(stringFields);
		Class newClass = buildSuperClassWithOnlyMethods(stringMethods);
		try {
			BaseJavaRuntime newInstance = (BaseJavaRuntime) newClass.newInstance();
			return newInstance;
		} catch (Exception e) {
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
	private BaseJavaRuntime buildRuntimeClass(List<String> stringMethods) {
		// evaluate the assimilator as an object
		ClassMaker maker = new ClassMaker();

		// add a super so we have a base method to execute
		maker.addSuper(superClassName);

		for (String stringMethod : stringMethods) {
			maker.addMethod(stringMethod);
		}

		Class<BaseJavaRuntime> newClass = maker.toClass();

		try {
			BaseJavaRuntime newInstance = newClass.newInstance();
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
			fw = new FileWriter("C:\\workspace\\Semoss_Dev\\src\\prerna\\sablecc2\\reactor\\test\\ClassTest.txt");
			writer = new BufferedWriter(fw);
//			writer.write("#################################    Class" + superClassesCount++
//					+ "###########################################\n");

			if (stringFields != null) {
				for (String stringField : stringFields) {
					writer.write(stringField);
					writer.write("\n");
				}
			}

			if (stringMethods != null) {
				for (String stringMethod : stringMethods) {
					writer.write(stringMethod);
					writer.write("\n");
				}
			}

			writer.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (stringFields != null) {
			for (String stringField : stringFields) {
				maker.addField(stringField);
			}
		}

		if (stringMethods != null) {
			for (String stringMethod : stringMethods) {
				maker.addMethod(stringMethod);
			}
		}

		Class<BaseJavaRuntime> newClass = maker.toClass();

		try {
			BaseJavaRuntime newInstance = newClass.newInstance();
			return newInstance;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	private List<String> buildMethods() {
		List<String> equationExecutionMethods = new ArrayList<>();
		int equationCount = 0;
		if (equations == null || equations.isEmpty()) {
			return equationExecutionMethods;
		}
		StringBuilder curMethod = new StringBuilder();
		// String varDefs = getVarDefinitions().toString();
		// curMethod.append(getInitMethod().toString());
		methodCount++;
		curMethod.append("private void execute" + methodCount + "() {" + "\n");
		// curMethod.append(varDefs);
		for (String equation : equations) {
			curMethod.append(equation + "\n");
			String varName = getVarNameFromEquation(equation);
			if (varName == "") {
				continue;
			}

			curMethod.append("a(\"" + varName + "\"," + varName + ");" + "\n");

			if (equationCount == maxEquationsPerMethod) {
				curMethod.append("}");
				equationExecutionMethods.add(curMethod.toString());

				equationCount = 0;
				methodCount++;
				curMethod = new StringBuilder();
				curMethod.append("private void execute" + methodCount + "() {" + "\n");
				// curMethod.append(varDefs);
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
		// method.append(getInitVarSettings());
		method.append("update();");
		method.append("super.execute();");
		// method.append("init();");
		for (int i = 1; i <= methodCount; i++) {
			method.append("execute" + i + "();");
		}
		method.append("}");

		return method.toString();
	}

	/**
	 * Build MainExcetution from start to end
	 * 
	 * @return
	 */
	private String buildMainExecutionMethod(int start, int end) {
		StringBuilder method = new StringBuilder();
		method.append("public void execute() {");
		// method.append(getInitVarSettings());
		method.append("update();");
		method.append("super.execute();");
		// method.append("init();");
		for (int i = start; i <= end; i++) {
			method.append("execute" + i + "();");
		}
		method.append("}");

		return method.toString();
	}

	private StringBuilder getEquationDefinitions() {
		StringBuilder equationDefs = new StringBuilder();
		for (String equation : equations) {
			equationDefs.append(equation + "\n");
			String varName = getVarNameFromEquation(equation);
			equationDefs.append("a(\"" + varName + "\"," + varName + ");");
		}
		return equationDefs;
	}

	private List<String> buildInitMethods() {
		List<String> initMethods = new ArrayList<>();
		int equationCount = 0;
		StringBuilder curMethod = new StringBuilder();
		methodCount = 1;
		curMethod.append("private void execute" + methodCount + "(){");
		for (String varName : variables) {
			curMethod.append("a(\"" + varName + "\"," + varName + ");");
			if (equationCount >= maxEquationsPerMethod) {
				curMethod.append("}");
				initMethods.add(curMethod.toString());
				curMethod = new StringBuilder();
				methodCount++;
				equationCount = 0;
				curMethod.append("private void execute" + methodCount + "(){");
			} else {
				equationCount++;
			}
		}
		curMethod.append("}");
		initMethods.add(curMethod.toString());

		return initMethods;
	}

	private String buildUpdateMethods() {
		StringBuilder updateMethod = new StringBuilder();
		if (equations != null && !equations.isEmpty()) {
			updateMethod.append("public void update(){");
			for (String equation : equations) {
				updateMethod.append(equation + "\n");
				String varName = getVarNameFromEquation(equation);
				if (varName == "") {
					continue;
				}
				updateMethod.append("a(\"" + varName + "\"," + varName + ");" + "\n");
			}
			updateMethod.append("}");
		}
		return updateMethod.toString();

	}

	private String getVarNameFromEquation(String equation) {
		return equation.split("=")[0].trim();
	}

	/**
	 * Serparate fields into several classes
	 * 
	 * @param fieldsList
	 * @param superClass
	 */
	private Class buildSuperClassWithOnlyFields(List<String> fieldsList) {
		Class superClass = null;
		for (List<String> partList : Lists.partition(fieldsList, maxFieldsPerClass)) {
			superClass = this.buildRuntimeClass(partList, null).getClass();
			superClassName = superClass.getName();
		}
		return superClass;
	}

	/**
	 * Sepeate methods into several classes
	 * 
	 * @param methods
	 * @param superClass
	 */
	private Class buildSuperClassWithOnlyMethods(List<String> stringMethods) {
		Class superClass = null;
		int start = 1;
		int end = stringMethods.size();
		for (List<String> partList : Lists.partition(stringMethods, maxMethodsPerClass)) {
			end = start + partList.size() - 1;
			String mainMethod = buildMainExecutionMethod(start, end);
			partList.add(mainMethod);
			superClass = this.buildRuntimeClass(null, partList).getClass();
			superClassName = superClass.getName();
			start = end + 1;
		}
		return superClass;
	}
}
