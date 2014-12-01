package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;

public class PieChartQueryBuilder extends AbstractQueryBuilder {
	static final Logger logger = LogManager.getLogger(PieChartQueryBuilder.class.getName());

	String label;
	String valueName;
	String valueMathFunc;
	
	public PieChartQueryBuilder (String label, String valueName, String valueMathFunc, ArrayList<Hashtable<String, String>> parameters, SEMOSSQuery baseQuery) {
		super(parameters, baseQuery);
		this.label = label;
		this.valueName = valueName;
		this.valueMathFunc = valueMathFunc;
	}
	
	@Override
	public void buildQuery () {
		ArrayList<String> varNames = uniqifyColNames(Arrays.asList(label, valueName));		
		
		//add label to query
		logger.info("Adding label: " + label + " with name: " + varNames.get(0));
		addReturnVariable(label, varNames.get(0), baseQuery, "false");
		
		// add the heat value
		logger.info("Adding value math function " + valueMathFunc + " on column " + valueName);
		addReturnVariable(valueName, varNames.get(1), baseQuery, valueMathFunc);
		
		//add them as group by
		ArrayList<String> groupList = new ArrayList<String>();
		groupList.add(label);
		logger.info("Query will GroupBy: " + groupList);
		SEMOSSQueryHelper.addGroupByToQuery(groupList, baseQuery);
		
		if(!parameters.isEmpty()) {
			logger.info("Adding parameters: " + parameters);
			addParam("Main");
		}
		
		createQuery();
		logger.info("Created PieChart Query: " + query);		
	}
}
