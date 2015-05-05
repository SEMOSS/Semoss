package prerna.poi.main.insights;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import prerna.algorithm.learning.similarity.GenerateEntropyDensity;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.QuestionAdministrator;
import prerna.rdf.engine.wrappers.WrapperManager;

public class AutoInsightCallable implements Callable<List<Object[]>> {

	//engine and question administrator to add questions
	private static AbstractEngine engine;
	private static QuestionAdministrator qa;

	//concept, perspectives + number of questions in perspective
	private String concept;
	private Hashtable<String, Integer> perspectiveIDHash;
	private List<InsightRule> rulesList;

	//property names and the table
	private String[] names;
	private ArrayList<Object []> propertyTable;

	//column types and entropies for each property
	private String[] colTypesArr;
	private double[] entropyArr;

	//related concept names, the directions and what is bidirectional
	private ArrayList<String> relatedConcepts;
	private ArrayList<String> conceptDirectionList;
	private Set<String> biDirectionalConceptsSet;

	//entropies and column types for all related concepts
	private ArrayList<ArrayList<Object[]>> conceptTableList;
	private ArrayList<String[]> conceptColTypesList;
	private ArrayList<double[]> conceptEntropyList;

	//rule requirements
	private Boolean hasAggregation;
	private String ruleQuestion;
	private String rulePerspective;
	private String ruleOutput;
	private String centralConcept;

	//list of params for the rule, as well as the params and constraints and possible assignments
	private List<String> params;
	private Hashtable<String, Hashtable<String,Object>> ruleConstraintHash;
	private Hashtable<String, String> paramClassHash;
	private Hashtable<String, Set<Integer>> possibleParamAssignmentsHash = new Hashtable<String, Set<Integer>>();

	private final String DOWNSTREAM = "downstream";
	private final String UPSTREAM = "upstream";

	private final String PROPERTIES_QUERY = "SELECT DISTINCT ?Prop WHERE {{?Concept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@CONCEPT@>}{?Prop <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>}{?Concept ?Prop ?PropVal }} ORDER BY ?Prop";
	private final String DOWNSTREAM_CONCEPTS_QUERY = "SELECT DISTINCT ?Concept2 WHERE {{?Concept2 <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>}{?Instance1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@CONCEPT@>}{?Instance1 <http://semoss.org/ontologies/Relation> ?Instance2 }{?Instance2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Concept2}} ORDER BY ?Concept2";
	private final String UPSTREAM_CONCEPTS_QUERY = "SELECT DISTINCT ?Concept2 WHERE {{?Concept2 <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>}{?Instance1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@CONCEPT@>}{?Instance2 <http://semoss.org/ontologies/Relation> ?Instance1 }{?Instance2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Concept2}} ORDER BY ?Concept2";
	private final String EMPTY_QUERY = "SELECT DISTINCT @RETVARS@ WHERE {{?@CONCEPT@ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@CONCEPT@>} @TRIPLES@} @ENDSTRING@";

	private final String PROPERTY_TRIPLE= "{?@CONCEPT@ <http://semoss.org/ontologies/Relation/Contains/@PROP@> ?@PROPCLEAN@}";
	private final String CONCEPT_DOWNSTREAM_TRIPLE= "{?@RELATEDCONCEPT@ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@RELATEDCONCEPT@>}{?@CONCEPT@ <http://semoss.org/ontologies/Relation> ?@RELATEDCONCEPT@}";
	private final String CONCEPT_UPSTREAM_TRIPLE= "{?@RELATEDCONCEPT@ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/@RELATEDCONCEPT@>}{?@RELATEDCONCEPT@ <http://semoss.org/ontologies/Relation> ?@CONCEPT@}";

	public AutoInsightCallable(AbstractEngine engine, QuestionAdministrator qa, String concept, List<InsightRule> rulesList, Hashtable<String, Integer> perspectiveIDHash) {
		AutoInsightCallable.engine = engine;
		AutoInsightCallable.qa = qa;
		this.concept = concept;
		this.rulesList = rulesList;
		this.perspectiveIDHash = perspectiveIDHash;
	}

	public static void addInsightsToXML(List<Object[]> insightList) {
		int i = 0;
		int length = insightList.size();
		for(; i < length; i++) {
			Object[] insight = insightList.get(i);
			String perspective = insight[0].toString();
			String questionKey = insight[1].toString();
			// index 2 is null
			String filledQuestion = insight[3].toString();
			String sparql = insight[4].toString();
			String ruleOutput = insight[5].toString();

			qa.cleanAddQuestion(perspective, questionKey, null, filledQuestion, sparql, ruleOutput, null, null, null, null);
		}
	}

	@Override
	public List<Object[]> call() {
		// keep the list of insights that should be added
		List<Object[]> insightList = new ArrayList<Object[]>();
		//find what number question to start with if the concept already has a perspective
		boolean propertyTableCreated = createPropertyTable(concept);
		boolean conceptTablesCreated = createConceptTables(concept);
		if(propertyTableCreated || conceptTablesCreated) {

			for(InsightRule rule : rulesList) {			
				paramClassHash = rule.getVariableTypeHash();
				params = new ArrayList<String>(paramClassHash.keySet());
				ruleConstraintHash = rule.getConstraints();
				ruleQuestion = rule.getQuestion();
				rulePerspective = rule.getPerspective();
				ruleOutput = rule.getOutput();
				centralConcept = rule.getCentralConcept();
				hasAggregation = rule.isHasAggregation();

				boolean allParamsMet = calculatePossibleParamAssignments();

				if(allParamsMet) {
					addAllPossibleInsights(0,new ArrayList<Integer>(),new HashSet<String>(), insightList);
				}
			}
		}	

		return insightList;
	}

	private boolean calculatePossibleParamAssignments() {
		//creates a mapping of the possible assignments for each parameter
		//returns false if there is any param that cannot be met, since the whole rule cannot be met
		possibleParamAssignmentsHash = new Hashtable<String, Set<Integer>>();

		//get the requirements for the central concept
		Hashtable<String,Object> centralConceptConstraintHash;
		if(ruleConstraintHash.containsKey(centralConcept)) {
			centralConceptConstraintHash = ruleConstraintHash.get(centralConcept);	
		}else {
			centralConceptConstraintHash = new Hashtable<String,Object>();
		}

		for(String param : params) {				
			String paramClass = paramClassHash.get(param);
			Hashtable<String,Object> paramConstraintHash = ruleConstraintHash.get(param);

			Set<Integer> possibleAssignments = new HashSet<Integer>();

			//if the parameter is a concept, go through all the concept tables
			//if the concept and the concept its related to meet the central and parameter reqs, respectively
			//add the possible assignment				
			if(paramClass.equals(InsightRuleConstants.CONCEPT_VALUE)) {

				int j;
				int relatedConceptsLength = relatedConcepts.size();
				for(j=0; j<relatedConceptsLength; j++) {

					String[] conceptColTypesArr  = conceptColTypesList.get(j);
					double[] conceptEntropyArr = conceptEntropyList.get(j);

					if(meetsParamRequirements(0,conceptColTypesArr,conceptEntropyArr,centralConceptConstraintHash)
							&& meetsParamRequirements(1,conceptColTypesArr,conceptEntropyArr,paramConstraintHash)) {
						possibleAssignments.add(j);
					}
				}

				//if the parameter is a property
				//AND if our concept meets the central concept constraints
				// go through all property table and fill with all properties that meet reqs.
			}else {

				if(names!=null && meetsParamRequirements(0,colTypesArr,entropyArr,centralConceptConstraintHash)) {
					int j = 1;
					int namesLength = names.length;
					for(; j<namesLength; j++) {
						if(meetsParamRequirements(j,colTypesArr,entropyArr,paramConstraintHash)) {
							possibleAssignments.add(j);
						}
					}
				}

			}

			if(possibleAssignments.size() == 0)
				return false;

			possibleParamAssignmentsHash.put(param,possibleAssignments);
		}
		return true;
	}

	private boolean createPropertyTable(String concept) {
		//query to get the properties related to this concept
		//construct a query to get all instances of this concept and their properties
		//create the table
		//calculate the column types and entropies

		propertyTable = new ArrayList<Object[]>();
		names = null;
		colTypesArr = null;
		entropyArr = null;

		String propQuery = PROPERTIES_QUERY.replaceAll("@CONCEPT@", concept);

		ArrayList<String> properties = getQueryResultsAsList(propQuery);
		if(properties.isEmpty())
			return false;

		int numVariables = properties.size() + 1;
		names = new String[numVariables];
		names[0] = concept;
		for(int i=1; i<numVariables; i++) {
			names[i] = properties.get(i-1);
		}

		String retVarString = "?"+concept;
		String triplesString = "";
		for(String prop : properties) {
			String propClean = prop.replaceAll("-","_");
			retVarString += " ?" + propClean;
			triplesString += "OPTIONAL" + PROPERTY_TRIPLE.replaceAll("@CONCEPT@",concept).replaceAll("@PROP@", prop).replaceAll("@PROPCLEAN@", propClean);
		}

		String tableQuery = buildQuery(retVarString,triplesString,"");

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, tableQuery);
		String[] vars = wrapper.getVariables();

		while(wrapper.hasNext())
		{
			ISelectStatement sjss = wrapper.next();

			Object [] values = new Object[vars.length];
			for(int colIndex = 0;colIndex < vars.length;colIndex++)
			{
				values[colIndex] = sjss.getVar(vars[colIndex]);
			}
			propertyTable.add(values);
		}

		GenerateEntropyDensity edGen = new GenerateEntropyDensity(propertyTable, true);
		colTypesArr = edGen.getColumnTypes();
		entropyArr = edGen.generateEntropy();

		return true;
	}

	private boolean createConceptTables(String concept) {
		//query to get the properties related to this concept
		//construct a query to pull the properties for each instance of concept
		//create the table

		relatedConcepts = new ArrayList<String>();
		conceptDirectionList = new ArrayList<String>();
		biDirectionalConceptsSet = new HashSet<String>();

		conceptTableList = new ArrayList<ArrayList<Object[]>>();
		conceptColTypesList = new ArrayList<String[]>();
		conceptEntropyList = new ArrayList<double[]>();

		String downstreamConceptQuery = DOWNSTREAM_CONCEPTS_QUERY.replaceAll("@CONCEPT@", concept);
		String upstreamConceptQuery = UPSTREAM_CONCEPTS_QUERY.replaceAll("@CONCEPT@", concept);

		addToRelatedConceptsList(downstreamConceptQuery, DOWNSTREAM);
		addToRelatedConceptsList(upstreamConceptQuery, UPSTREAM);


		if(relatedConcepts.isEmpty())
			return false;

		//create a set of any concepts that go in both directions
		int i=0;
		int relatedConceptsLength = relatedConcepts.size();

		for(; i<relatedConceptsLength; i++ ) {
			String relatedConcept = relatedConcepts.get(i);

			if(relatedConcepts.lastIndexOf(relatedConcept) != i) {
				biDirectionalConceptsSet.add(relatedConcept);
			}

			String retVarString = "?" + concept + " ?" + relatedConcept;
			String triplesString;

			String direction = conceptDirectionList.get(i);
			if(direction.equals(DOWNSTREAM))
				triplesString = CONCEPT_DOWNSTREAM_TRIPLE.replaceAll("@CONCEPT@", concept).replaceAll("@RELATEDCONCEPT@", relatedConcept);
			else
				triplesString = CONCEPT_UPSTREAM_TRIPLE.replaceAll("@CONCEPT@", concept).replaceAll("@RELATEDCONCEPT@", relatedConcept);


			String relatedConceptInstanceQuery = buildQuery(retVarString,triplesString,"");

			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, relatedConceptInstanceQuery);
			String[] wrapNames = wrapper.getVariables();

			ArrayList<Object[]> relatedList = new ArrayList<Object[]>();
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();

				Object [] values = new Object[wrapNames.length];
				for(int colIndex = 0;colIndex < wrapNames.length;colIndex++)
				{
					values[colIndex] = sjss.getVar(wrapNames[colIndex]);
				}
				relatedList.add(values);
			}
			conceptTableList.add(relatedList);

			GenerateEntropyDensity edGen = new GenerateEntropyDensity(relatedList, true);
			conceptColTypesList.add(edGen.getColumnTypes());
			conceptEntropyList.add(edGen.generateEntropy());
		}

		return true;
	}

	private void addToRelatedConceptsList(String query, String direction) {

		ArrayList<String> relatedConceptsToAdd = getQueryResultsAsList(query);
		relatedConceptsToAdd.remove("Concept");
		relatedConceptsToAdd.remove(concept);//TODO handle when concept related to concept

		int i=0;
		int numConcepts = relatedConceptsToAdd.size();
		for(; i<numConcepts; i++) {
			conceptDirectionList.add(direction);
		}

		this.relatedConcepts.addAll(relatedConceptsToAdd);
	}

	private ArrayList<String> getQueryResultsAsList(String query) {

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		// get the bindings from it
		String[] names = wrapper.getVariables();
		ArrayList<String> properties = new ArrayList<String>();

		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			ISelectStatement sjss = wrapper.next();
			properties.add(sjss.getVar(names[0]).toString());
		}

		return properties;
	}

	//checks to see if a given column in the table meets a given set of requirement
	private boolean meetsParamRequirements(int colIndex, String[] colTypesArr, double[] entropyArr, Hashtable<String,Object> requirementHash) {

		//classes are the same and no constraints so return true
		if(requirementHash == null || requirementHash.isEmpty()) {
			return true;
		}

		Object paramType = requirementHash.get(InsightRuleConstants.DATA_TYPE);
		if(paramType != null && !paramType.toString().equals(colTypesArr[colIndex]))
			return false;

		Object entropyMax = requirementHash.get(InsightRuleConstants.ENTROPY_DENSITY_MAX);
		if(entropyMax != null && entropyArr[colIndex] > Double.parseDouble(entropyMax.toString()))
			return false;

		Object entropyMin = requirementHash.get(InsightRuleConstants.ENTROPY_DENSITY_MIN);
		if(entropyMin!=null && entropyArr[colIndex] <  Double.parseDouble(entropyMin.toString()))
			return false;

		return true;

	}

	/**
	 * Recursive method to determine the assignment for a parameter.
	 * Fills the next spot in the assignment list and recalls the method.
	 * Each combination of assignments can be used to make one insight.
	 * i.e. makes sure that the same insight is not created with params in a different order
	 * @param assignments
	 * @param parentUsedAssignments
	 */
	private void addAllPossibleInsights(int index,List<Integer> assignments, Set<String> parentUsedAssignments, List<Object[]> insightList) {
		//if all params have an assignment, then create the insight with the assignments
		//otherwise, assign the current param and recall the method
		if(index == params.size()) {
			createInsight(assignments, insightList);
		}else {

			Set<String> usedAssignments = new HashSet<String>(parentUsedAssignments);

			String currParam = params.get(index);
			String paramClass = paramClassHash.get(currParam);

			Set<Integer> possibleAssignments = possibleParamAssignmentsHash.get(currParam);

			if(possibleAssignments != null) {
				Iterator<Integer> itr = possibleAssignments.iterator();
				while(itr.hasNext()) {
					Integer possibleAssignment = itr.next();

					//only continue if no previous parameters have been assigned this assignment
					if(!usedAssignments.contains(paramClass+"-"+possibleAssignment)) {
						usedAssignments.add(paramClass+"-"+possibleAssignment);
						assignments.add(possibleAssignment);
						addAllPossibleInsights(index+1,assignments,usedAssignments, insightList);
						assignments.remove(possibleAssignment);
					}
				}
			}
		}

	}

	private void createInsight(List<Integer> assignments, List<Object[]> insightList) {
		//process through all the params
		//ignore the concept
		//if property
		//then add to retvar and add to triples string
		int i;
		int paramSize = params.size();
		String retVarString = "";
		String triplesString = "";
		String endString = "";

		String filledQuestion = ruleQuestion;
		String perspective = concept+"-Perspective";

		for(i=0; i<paramSize; i++) {
			String param = params.get(i);
			String classType = paramClassHash.get(param);

			String var;
			String varClean;
			if(classType.equals(InsightRuleConstants.PROPERTY_VALUE)) {
				var = names[assignments.get(i)];
				varClean = var.replaceAll("-","_");

				triplesString += PROPERTY_TRIPLE.replaceAll("@CONCEPT@",concept).replaceAll("@PROP@", var).replaceAll("@PROPCLEAN@", varClean);

				filledQuestion = filledQuestion.replaceAll("\\$"+ params.get(i), var);

			} else {
				var = relatedConcepts.get(assignments.get(i));
				varClean = var.replaceAll("-","_");

				String direction = conceptDirectionList.get(assignments.get(i));
				if(direction.equals(DOWNSTREAM))
					triplesString += CONCEPT_DOWNSTREAM_TRIPLE.replaceAll("@CONCEPT@", concept).replaceAll("@RELATEDCONCEPT@", var);
				else
					triplesString += CONCEPT_UPSTREAM_TRIPLE.replaceAll("@CONCEPT@", concept).replaceAll("@RELATEDCONCEPT@", var);

				if(biDirectionalConceptsSet.contains(var)) {
					filledQuestion = filledQuestion.replaceAll("\\$"+ params.get(i), direction + " " + var);
				}else {
					filledQuestion = filledQuestion.replaceAll("\\$"+ params.get(i), var);
				}
			}

			String aggregationType = "";
			if(ruleConstraintHash.containsKey(param) && ruleConstraintHash.get(param).containsKey(InsightRuleConstants.AGGREGATION))
				aggregationType = ruleConstraintHash.get(param).get(InsightRuleConstants.AGGREGATION).toString();
			//if no aggregation for insight -> basic return string
			//if aggregation for insight but not this param -> basic return string and end string
			//if aggregation for this insight and this param -> complicated return string
			if(!hasAggregation) {
				retVarString += " ?" + varClean;
				endString += " ?" + varClean;
			} else if(aggregationType.length() == 0) {
				retVarString = " ?" + varClean + retVarString;
				endString += " ?" + varClean;
			} else {
				retVarString += " (" + aggregationType.toString() + "(?" + varClean + ") AS ?" + varClean + "_" + aggregationType.toString() + ")";
			}

			if(rulePerspective.equals(param))
				perspective = var + "-Perspective";
		}

		//add the central concept to the return string. //TODO the group by string too?
		if(!hasAggregation || endString.length() == 0) {
			retVarString = " ?"+concept + retVarString;
			endString += " ?" + concept;
		} else if(ruleConstraintHash.containsKey(centralConcept) && ruleConstraintHash.get(centralConcept).containsKey(InsightRuleConstants.AGGREGATION)) {
			//if there is some type of aggregation on the central concept
			String aggregationType = ruleConstraintHash.get(centralConcept).get(InsightRuleConstants.AGGREGATION).toString();
			retVarString += " (" + aggregationType.toString() + "(?" + concept + ") AS ?" + concept + "_" + aggregationType.toString() + ")";
		}

		if(!hasAggregation) {
			endString = "ORDER BY" + endString;
		}else {
			endString = "GROUP BY" + endString + " ORDER BY" + endString;
		}

		String sparql = buildQuery(retVarString, triplesString, endString);

		filledQuestion = filledQuestion.replaceAll("\\$"+ centralConcept, concept);

		int id;
		if(perspectiveIDHash.containsKey(perspective)) {
			id = perspectiveIDHash.get(perspective) + 1;
		} else {
			id = 1;
		}

		perspectiveIDHash.put(perspective, id);
		String questionKey = perspective + "_" + id;

		Object[] insight = new Object[]{perspective, questionKey, null, filledQuestion, sparql, ruleOutput};
		insightList.add(insight);
	}

	private String buildQuery(String retVarString, String triplesString, String endString) {
		String ret = EMPTY_QUERY.replaceAll("@CONCEPT@", concept).replaceAll("@RETVARS@", retVarString).replaceAll("@TRIPLES@",triplesString).replaceAll("@ENDSTRING@",endString);
		return ret;
	}
}
