package prerna.sablecc2.reactor.planner.graph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.BaseJavaRuntime;
import prerna.sablecc2.reactor.PKSLPlanner;

public class ExecuteJavaGraphPlannerReactor extends AbstractPlannerReactor {

	private static final Logger LOGGER = LogManager.getLogger(ExecuteJavaGraphPlannerReactor.class.getName());
	
	@Override
	public NounMetadata execute()
	{
		long start = System.currentTimeMillis();
		
		PKSLPlanner planner = getPlanner();
		List<String> pksls = new Vector<String>();

		// using the root vertices
		// iterate down all the other vertices and add the signatures
		// for the desired travels in the appropriate order
		// note: this is adding to the list of undefined variables
		// calculated at beginning of class 
		traverseDownstreamVertsProcessor(planner, pksls);
		
		//now we can the pksls
		
		Map<String, String> mainMap = (Map)planner.getProperty("MAIN_MAP", "MAIN_MAP");
		
		List<String> fieldsList = buildFields(mainMap, planner);
		
		long startTime = System.currentTimeMillis();
		RuntimeJavaClassBuilder builder = new RuntimeJavaClassBuilder();
		builder.addEquations(pksls);
		builder.addFields(fieldsList);
		BaseJavaRuntime javaClass = builder.buildClass();
		javaClass.execute();
		Map<String, Object> map = javaClass.getVariables();
		for(String key : map.keySet()) {
			System.out.println(key+":::"+map.get(key));
		}
		long endTime = System.currentTimeMillis();
		
		long end = System.currentTimeMillis();
		LOGGER.info("****************    END RUN PLANNER "+(end - start)+"ms      *************************");
		
		return new NounMetadata(javaClass, PkslDataTypes.PLANNER);
	}
	
	private PKSLPlanner getPlanner() {
		GenRowStruct allNouns = getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
		PKSLPlanner planner = null;
		if(allNouns != null) {
			planner = (PKSLPlanner) allNouns.get(0);
			return planner;
		} else {
			return this.planner;
		}
	}
	
	private List<String> buildFields(Map<String, String> mainMap, PKSLPlanner planner) {
		List<String> fields = new ArrayList<>();
		Set<String> assignedFields = new HashSet<>();
		
		for(String assignment : mainMap.keySet()) {
			String value = mainMap.get(assignment);
			
			boolean isNumber = isNumber(value);
			
			while(!isNumber && value != null && (!value.equals("double") && !value.equals("boolean") && !value.equals("String") && !value.equals("int"))) {
				value = mainMap.get(value);
			}
			
			if(value == null) {
				NounMetadata noun = planner.getVariableValue(assignment);
				if(noun != null) {
					PkslDataTypes nounType = noun.getNounName();
					String field = "";
					Object nounValue = noun.getValue();
					if(nounType == PkslDataTypes.CONST_DECIMAL || nounType == PkslDataTypes.CONST_INT) {
						field = "double "+assignment+" = "+nounValue+";";
					} else if(nounType == PkslDataTypes.CONST_STRING) {
						field = "String "+assignment+" = \""+nounValue+"\";";
					} else if(nounType == PkslDataTypes.BOOLEAN) {
						field = "boolean "+assignment+" = "+nounValue+";";
					}
					
					if(!assignedFields.contains(assignment)) {
						fields.add(field);
						assignedFields.add(assignment);
					}
					
				} else {
					
				}
			} else if(isNumber) { 
				String field = "public double "+" "+assignment + " = "+value+";";
				if(!assignedFields.contains(assignment)) {
					fields.add(field);
					assignedFields.add(assignment);
				}
			} else {
				String field = "public "+value+" "+assignment;
				if(value.equals("double") || value.equals("int")) {
					field += " = 0.0;";
				} else if(value.equals("String")) {
					field += " = \"\";";
				} else if(value.equals("boolean")) {
					field += " = true;";
				} 
				
				if(!assignedFields.contains(assignment)) {
					fields.add(field);
					assignedFields.add(assignment);
				}
			}
		}
		
		for(String assignment : planner.getVariables()) {
			NounMetadata noun = planner.getVariableValue(assignment);
			if(noun != null) {
				PkslDataTypes nounType = noun.getNounName();
				Object nounValue = noun.getValue();
				String field = "";
				if(nounType == PkslDataTypes.CONST_DECIMAL || nounType == PkslDataTypes.CONST_INT) {
					field = "double "+assignment+" = "+nounValue+";";
				} else if(nounType == PkslDataTypes.CONST_STRING) {
					field = "String "+assignment+" = \""+nounValue+"\";";
				} else if(nounType == PkslDataTypes.BOOLEAN) {
					field = "boolean "+assignment+" = "+nounValue+";";
				}
				
				if(!assignedFields.contains(assignment)) {
					fields.add(field);
					assignedFields.add(assignment);
				}
			} else {
				
			}
		}
		return fields;
	}
		
	private boolean isNumber(String value) {
		try {
			double doub = Double.parseDouble(value);
			return true;
		} catch(Exception e) {
			return false;
		}
	}
	
	/**
	 * We want to override the pksl that is extracted from the vertex so that we grab the java string instead of the pksl string
	 */
	@Override
	protected String getPksl(Vertex vert) {
		
		//get the java signatures
		try {
			if(vert.property("JAVA_SIGNATURE") != null) {
				String pkslOperation = vert.value("JAVA_SIGNATURE");
				if(pkslOperation.isEmpty()) {
					return pkslOperation;
				}
				else return pkslOperation+";";
			}
			return "";
		} catch(Exception e) {
			return "";
		}
	}

}
