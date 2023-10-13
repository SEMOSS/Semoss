package prerna.reactor.planner.graph;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import com.google.common.collect.Lists;

import prerna.reactor.BaseJavaRuntime;
import prerna.reactor.ClassMaker;
import prerna.util.Utility;

public class RuntimeJavaClassBuilder {

	private String packageName;
	private String className;
	
	// this will hold the equations we need to push into the methods
	protected List<String> equations = new Vector<String>();
	// this will hold the fields required for this class
	protected List<String> fields = new Vector<String>();
	// this will hold the variables used by this class
	protected Set<String> variables;
	// this will hold the count for the methods used
	// we need this because each method has a max size based on javasist
	protected int methodCount;
	// this will hold the name of the super class
	protected String superClassName;
	// this is so we keep within javasist limits
	protected int maxEquationsPerMethod = 700;
	protected int maxFieldsPerClass = 7000;
	protected int maxMethodsPerClass = 30;

	// used if we want to write the file for testing
	private boolean testing = false;

	/**
	 * Default constructor
	 * Will set the super as BaseJavaRuntime
	 */
	public RuntimeJavaClassBuilder() {
		this(Utility.getRandomString(12), Utility.getRandomString(12));
	}
	
	/**
	 * Default constructor
	 * Will set the super as BaseJavaRuntime
	 */
	public RuntimeJavaClassBuilder(String packageName, String className) {
		this.packageName = packageName;
		this.className = className;
		this.superClassName = BaseJavaRuntime.class.getName();
	}

	/**
	 * Set the super class for this class
	 * Useful when the class is to large
	 * and we need to make additional super classes
	 * @param superClass
	 */
	public void setSuperClass(Class superClass) {
		this.superClassName = superClass.getName();
	}

	/**
	 * used when you only want fields and no methods
	 * @return
	 */
	public Class<BaseJavaRuntime> generateClassWithOnlyFields() {
		// just use the fields
		// pass in empty list for the methods
		return buildRuntimeClass(this.fields, new Vector<String>());
	}

	/**
	 * Used for class which only contains methods
	 * @return
	 */
	public Class<BaseJavaRuntime> generateClassWithOnlyMethods() {
		List<String> methods = buildInitMethods();
		methods.addAll(buildMethods());
		return buildRuntimeClassSeperately(this.fields, methods);
	}

	/**
	 * Build the class
	 * Uses both fields and methods
	 * @return
	 */
	public Class<BaseJavaRuntime> generateClass() {
		List<String> methods = buildInitMethods();
		methods.addAll(buildMethods());
		methods.add(buildMainExecutionMethod());
		return buildRuntimeClass(this.fields, methods);
	}

	/**
	 * Builds a class to update values in an existing class
	 * @return
	 */
	public Class<BaseJavaRuntime> generateUpdateClass() {
		List<String> methods = new ArrayList<String>();
		methods.add(buildUpdateMethods());
		methods.add(buildMainExecutionMethod());
		return buildRuntimeClass(this.fields, methods);
	}

	/**
	 * Add to the list of equations currently being kept
	 * These equations are pushed into methods
	 * @param equations
	 */
	public void addEquations(List<String> equations) {
		for (String equation : equations) {
			if (!equations.isEmpty()) {
				this.equations.add(equation);
			}
		}
	}

	/**
	 * Add to the list of fields
	 * These fields are defined at the beginning of the class
	 * @param fields
	 */
	public void addFields(List<String> fields) {
		for (String field : fields) {
			if (!field.isEmpty()) {
				this.fields.add(field);
			}
		}
	}

	/**
	 * This is the main class for generating the model
	 * @param stringFields			This is the list of fields
	 * @param stringMethods			This is the list of methods
	 * @return
	 */
	private Class<BaseJavaRuntime> buildRuntimeClass(List<String> stringFields, List<String> stringMethods) {
		ClassMaker maker = new ClassMaker(this.packageName, this.className);
		// add a super so we have a base method to execute
		maker.addSuper(superClassName);
		// loop through and add all the fields
		if (stringFields != null) {
			for (String stringField : stringFields) {
				maker.addField(stringField);
			}
		}

		// loop through and add the methods
		if (stringMethods != null) {
			for (String stringMethod : stringMethods) {
				maker.addMethod(stringMethod);
			}
		}

		// generate the class
		Class<BaseJavaRuntime> generatedClass = maker.toClass();

		// for testing, we can write out the class if we would like
		if(testing) {
			writeClassForTesting(stringFields, stringMethods);
		}

		return generatedClass;
	}

	/**
	 * Build the methods that we need to add using the list of equations
	 * @return
	 */
	private List<String> buildMethods() {
		List<String> equationExecutionMethods = new ArrayList<>();
		// if we have no equations... this is simple
		if (this.equations == null || this.equations.isEmpty()) {
			return equationExecutionMethods;
		}

		// we have equations
		// we need to keep track of the size such that this will compile
		this.methodCount++;
		int equationCount = 0;
		// each method gets its own string builder
		StringBuilder curMethod = new StringBuilder();

		// each method gets execute + i, where i is an integer
		curMethod.append("private void execute" + this.methodCount + "() {");
		for (String equation : equations) {
			curMethod.append(equation);
			String varName = getVarNameFromEquation(equation);
			if (varName == "") {
				continue;
			}

			// method a is to store the variable
			// so we call that after each formula
			// we put the actual name to the variable reference as a result of the name
			curMethod.append("a(\"" + varName + "\"," + varName + ");");
			// if we have the max equation amount for this method, move on
			if (equationCount == this.maxEquationsPerMethod) {
				// end the method
				curMethod.append("}");
				// add it ot the list
				equationExecutionMethods.add(curMethod.toString());

				// create a new method
				equationCount = 0;
				this.methodCount++;
				curMethod = new StringBuilder();
				curMethod.append("private void execute" + this.methodCount + "() {");
			} else {
				equationCount++;
			}
		}
		// finish the last method
		curMethod.append("}");
		// add it to the list
		equationExecutionMethods.add(curMethod.toString());
		return equationExecutionMethods;
	}

	/**
	 * Create a execute method that will call all the 
	 * execute + i, where i is an integer methods
	 * since there is a max size for each method
	 * @return
	 */
	private String buildMainExecutionMethod() {
		StringBuilder method = new StringBuilder();
		method.append("public void execute() {super.update(); update(); super.execute();");
		for (int i = 1; i <= methodCount; i++) {
			method.append("execute" + i + "();");
		}
		method.append("}");
		return method.toString();
	}

	/**
	 * All the fields that we are using
	 * They need to be pushed into the map
	 * For retrieval later on
	 * @return
	 */
	private List<String> buildInitMethods() {
		List<String> initMethods = new Vector<>();
		StringBuilder curMethod = new StringBuilder();

		this.methodCount = 1;
		int equationCount = 0;

		// this will iterate through and add all the variables into the map
		// also keeping in track the max size
		curMethod.append("private void execute" + this.methodCount + "(){");
		for (String varName : variables) {
			curMethod.append("a(\"" + varName + "\"," + varName + ");");
			if (equationCount >= maxEquationsPerMethod) {
				curMethod.append("}");
				initMethods.add(curMethod.toString());
				curMethod = new StringBuilder();
				this.methodCount++;
				equationCount = 0;
				curMethod.append("private void execute" + this.methodCount + "(){");
			} else {
				equationCount++;
			}
		}
		curMethod.append("}");
		initMethods.add(curMethod.toString());

		return initMethods;
	}

	/**
	 * Generate the update method
	 * @return
	 */
	private String buildUpdateMethods() {
		// will implement the update method
		// assumption, since this is done via the user clicking on the UI
		// no way this should be large enough to cause any issues with size
		StringBuilder updateMethod = new StringBuilder();
		if (equations != null && !equations.isEmpty()) {
			updateMethod.append("public void update(){");
			for(String equation : equations) {
				updateMethod.append(equation);
				String varName = getVarNameFromEquation(equation);
				if (varName == "") {
					continue;
				}
				updateMethod.append("a(\"" + varName + "\"," + varName + ");");
			}
			updateMethod.append("}");
		}
		return updateMethod.toString();
	}

	/**
	 * Split the string to get the variable name being used
	 * @param equation
	 * @return
	 */
	private String getVarNameFromEquation(String equation) {
		return equation.split("=")[0].trim();
	}

	/**
	 * 
	 * @param stringFields
	 * @param stringMethods
	 * @return
	 */
	private Class<BaseJavaRuntime> buildRuntimeClassSeperately(List<String> stringFields, List<String> stringMethods) {
		buildSuperClassWithOnlyFields(stringFields);
		Class<BaseJavaRuntime> generatedClass = buildSuperClassWithOnlyMethods(stringMethods);
		return generatedClass;
	}

	/**
	 * Build MainExcetution from start to end
	 * 
	 * @return
	 */
	private String buildMainExecutionMethod(int start, int end) {
		StringBuilder method = new StringBuilder();
		method.append("public void execute() {super.update();update();super.execute();");
		for (int i = start; i <= end; i++) {
			method.append("execute" + i + "();");
		}
		method.append("}");
		return method.toString();
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

	/**
	 * Used only for testing!!!
	 * 
	 * @param stringFields
	 * @param stringMethods
	 */
	public void writeClassForTesting(List<String> stringFields, List<String> stringMethods) {
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

			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(writer != null) {
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}
			if(fw != null) {
			try {
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}
		}
	}

}
