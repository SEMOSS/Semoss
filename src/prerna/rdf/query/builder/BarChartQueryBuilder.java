package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;

public class BarChartQueryBuilder extends AbstractQueryBuilder {
	static final Logger logger = LogManager.getLogger(BarChartQueryBuilder.class.getName());
	String labelColName;
	ArrayList<String> valueColNames;
	ArrayList<String> valueMathFunctions;
	
	public BarChartQueryBuilder (String labelColName, ArrayList<String> valueColNames, ArrayList<String> valueMathFunctions, ArrayList<Hashtable<String, String>> parameters, SEMOSSQuery baseQuery) {
		super(parameters, baseQuery);
		this.labelColName = labelColName;
		this.valueColNames = valueColNames;
		this.valueMathFunctions = valueMathFunctions;
	}
	
	@Override
	public void buildQuery () {
		ArrayList<String> allColNames = new ArrayList<String>();
		ArrayList<String> groupList = new ArrayList<String>();
		allColNames.add(labelColName);
		allColNames.addAll(valueColNames);
		
		ArrayList<String> allVarNames = uniqifyColNames(allColNames );
		logger.info("Unique variables are: " + allVarNames);
		
		//first add the label as return var to query
		logger.info("Adding Return Variable: " + labelColName);
		addReturnVariable(labelColName, allVarNames.get(0), baseQuery, "false");
		groupList.add(labelColName);
		
		//now iterate through to all series values with their math functions
		for(int seriesIdx = 0 ; seriesIdx < valueColNames.size(); seriesIdx++){
			String mathFunc = valueMathFunctions.get(seriesIdx);
			String seriesColName = valueColNames.get(seriesIdx);

			logger.info("Adding Return Variable: " + seriesColName);
			addReturnVariable(seriesColName, allVarNames.get(seriesIdx+1), baseQuery, mathFunc);
			
			if(mathFunc.equals("false")){
				groupList.add(seriesColName);
			}
		}

		logger.info("Adding GROUPBY to query: " + allColNames);
		SEMOSSQueryHelper.addGroupByToQuery(groupList, baseQuery);
		
		if(!parameters.isEmpty()) {
			logger.info("Adding parameters: " + parameters);
			addParam("Main");
		}
		
		createQuery();
		logger.info("Created BarChart Query: " + query);		
	}
}
