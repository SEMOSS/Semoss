package prerna.sablecc2.reactor.planner.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.BaseJavaRuntime;
import prerna.sablecc2.reactor.PKSLPlanner;

public class ExecuteJavaGraphPlannerReactor extends AbstractPlannerReactor {

	private static final Logger LOGGER = LogManager.getLogger(ExecuteJavaGraphPlannerReactor.class.getName());

	@Override
	public NounMetadata execute() {
		long start = System.currentTimeMillis();

		PKSLPlanner planner = getPlanner();
		PKSLPlanner basePlanner = getBasePlanner();
		List<String> pksls = new LinkedList<String>();
		Class<BaseJavaRuntime> superClass = null;
		Map<String, String> mainMap = (Map) planner.getProperty("MAIN_MAP", "MAIN_MAP");
		if (basePlanner != null) {
			// We are excuting a plan based on a base plan
			// 1. copy the Main Map to Base Map
			// 2. Get the super class from the base plan
			superClass = (Class<BaseJavaRuntime>) basePlanner.getProperty("RUN_CLASS", "RUN_CLASS");
			Map<String, String> baseMap = new HashMap<String, String>((HashMap<String, String>) basePlanner.getProperty("MAIN_MAP", "MAIN_MAP"));
			planner.addProperty("BASE_MAP", "BASE_MAP", baseMap);

			// Exclude nodes already in base plan
			mainMap.keySet().removeAll(baseMap.keySet());
		}

		// using the root vertices
		// iterate down all the other vertices and add the signatures
		// for the desired travels in the appropriate order
		// note: this is adding to the list of undefined variables
		// calculated at beginning of class
		traverseDownstreamVertsProcessor(planner, pksls);

		// now we can the pksls

		RuntimeJavaClassBuilder builder = new RuntimeJavaClassBuilder();
		long startTime = System.currentTimeMillis();
		List<String> fieldsList = buildFields(mainMap, planner, builder);
		long endTime = System.currentTimeMillis();
		LOGGER.info("****************    Build fields " + (endTime - startTime) + "ms      *************************");

		startTime = System.currentTimeMillis();
		superClass = buildSuperClassWithOnlyFields(fieldsList, superClass);
		endTime = System.currentTimeMillis();
		LOGGER.info("****************    Build Super Clases with Fields " + (endTime - startTime) + "ms      *************************");
		
		startTime = System.currentTimeMillis();
		builder.addEquations(pksls);
		endTime = System.currentTimeMillis();
		LOGGER.info("****************    Add equations " + (endTime - startTime) + "ms      *************************");

		startTime = System.currentTimeMillis();
		builder.addFields(fieldsList);
		endTime = System.currentTimeMillis();
		LOGGER.info("****************    Add equations " + (endTime - startTime) + "ms      *************************");

		startTime = System.currentTimeMillis();
		builder.setSuperClass(superClass);
		endTime = System.currentTimeMillis();
		LOGGER.info("****************    Set super " + (endTime - startTime) + "ms      *************************");
		
		startTime = System.currentTimeMillis();
		Class<BaseJavaRuntime> javaClass = builder.generateClass();
		endTime = System.currentTimeMillis();
		LOGGER.info("****************    Build Class " + (endTime - startTime) + "ms      *************************");

		planner.addProperty("RUN_CLASS", "RUN_CLASS", javaClass);
		return new NounMetadata(planner, PkslDataTypes.PLANNER);
	}

	private PKSLPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		PKSLPlanner planner = null;
		if (allNouns != null) {
			planner = (PKSLPlanner) allNouns.get(0);
			return planner;
		} else {
			return this.planner;
		}
	}

	private Class<BaseJavaRuntime> buildSuperClassWithOnlyFields(List<String> fieldsList, Class<BaseJavaRuntime> superClass) {
		for (List<String> partList : Lists.partition(fieldsList, fieldsList.size()/2)) {
			RuntimeJavaClassBuilder superClassBuilder = new RuntimeJavaClassBuilder();
			superClassBuilder.addFields(partList);
			if(superClass != null){
				superClassBuilder.setSuperClass(superClass);
			}
			superClass = superClassBuilder.generateClassWithOnlyFields();
		}
		fieldsList.clear();
		return superClass;
	}

	private List<String> buildFields(Map<String, String> mainMap, PKSLPlanner planner,
			RuntimeJavaClassBuilder builder) {
		List<String> fields = new LinkedList<>();
		Set<String> assignedFields = new HashSet<>();
		for (String assignment : planner.getVariables()) {
			if (!assignedFields.contains(assignment)) {
				NounMetadata noun = planner.getVariableValue(assignment);
				if (noun != null) {
					PkslDataTypes nounType = noun.getNounName();
					Object nounValue = noun.getValue();
					String field = "";
					// String addToMapString = "{a(\"" + assignment + "\"," +
					// assignment + ");}";
					boolean isValidTypeFlag = false;
					if (nounType == PkslDataTypes.CONST_DECIMAL || nounType == PkslDataTypes.CONST_INT) {
						field = "public double " + assignment + " = " + nounValue + ";";
						isValidTypeFlag = true;
					} else if (nounType == PkslDataTypes.CONST_STRING) {
						field = "public String " + assignment + " = \"" + nounValue + "\";";
						isValidTypeFlag = true;
					} else if (nounType == PkslDataTypes.BOOLEAN) {
						field = "public boolean " + assignment + " = " + nounValue + ";";
						isValidTypeFlag = true;
					}
					if (isValidTypeFlag) {
						fields.add(field);
						assignedFields.add(assignment);
					}

				}
			}

		}

		for (String assignment : mainMap.keySet()) {
			if (!assignedFields.contains(assignment)) {
				String value = mainMap.get(assignment);
				String field = "public " + value + " " + assignment;
				boolean isValidType = false;
				if (value.equals("double") || value.equals("int")) {
					field += " = 0.0;";
					isValidType = true;
				} else if (value.equals("String")) {
					field += " = \"\";";
					isValidType = true;
				} else if (value.equals("boolean")) {
					field += " = true;\n";
					isValidType = true;
				}
				if (isValidType) {
					fields.add(field);
					assignedFields.add(assignment);
				}
			}
		}
		builder.variables = assignedFields;
		return fields;
	}

	/**
	 * We want to override the pksl that is extracted from the vertex so that we
	 * grab the java string instead of the pksl string
	 */
	@Override
	protected String getPksl(Vertex vert) {
		// get the java signatures
		try {
			if (vert.property("JAVA_SIGNATURE") != null) {
				String pkslOperation = vert.value("JAVA_SIGNATURE");
				if (pkslOperation.isEmpty()) {
					return pkslOperation;
				} else
					return pkslOperation + ";";
			}
			return "";
		} catch (Exception e) {
			return "";
		}
	}

	/**
	 * Get the Base Plan passed as the second Parameter
	 * 
	 * @return
	 */
	protected PKSLPlanner getBasePlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		if (allNouns != null && allNouns.size() > 1) {
			Object secondParam = allNouns.get(1);
			if (secondParam != null) {
				PKSLPlanner basePlan = (PKSLPlanner) secondParam;
				return basePlan;
			}
		}
		return null;
	}

}
