package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.query.util.SEMOSSQuery;

public class GenericTableQueryBuilder extends AbstractQueryBuilder{
	static final Logger logger = LogManager.getLogger(GenericTableQueryBuilder.class.getName());
	
	ArrayList<String> labelList;
	
	public GenericTableQueryBuilder (ArrayList<String> labelList, ArrayList<Hashtable<String, String>> parameters, SEMOSSQuery baseQuery) {
		super(parameters, baseQuery);
		this.labelList = labelList;
	}

	@Override
	public void buildQuery() {
		// TODO Auto-generated method stub
		ArrayList<String> varNames = uniqifyColNames(labelList);
		
		for(int i=0; i < labelList.size(); i++){
			String varName = labelList.get(i);
			
			logger.info("Adding variable: " + varName);
			addReturnVariable(varName, varNames.get(i), baseQuery, "false");
		}
		
		if(!parameters.isEmpty()) {
			logger.info("Adding parameters: " + parameters);
			addParam("Main");
		}
		
		createQuery();
		logger.info("Created Generic Table Query: " + query);		
	}
	
}
