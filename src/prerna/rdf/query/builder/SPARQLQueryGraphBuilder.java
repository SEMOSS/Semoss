package prerna.rdf.query.builder;

import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.rdf.query.util.SEMOSSQueryHelper;
import prerna.rdf.query.util.SPARQLConstants;

public class SPARQLQueryGraphBuilder extends AbstractSPARQLQueryBuilder {
	
	static final Logger logger = LogManager.getLogger(SPARQLQueryGraphBuilder.class.getName());

	IEngine engine = null;

	public SPARQLQueryGraphBuilder(IEngine engine){
		super(engine);
		this.engine = engine;
	}
	
	protected void addRelationshipTriples (List<Hashtable<String,String>> predV) {
		for(Hashtable<String, String> predHash : predV){
			String predName = predHash.get(QueryBuilderHelper.varKey);
			String predURI = predHash.get(QueryBuilderHelper.uriKey);

			SEMOSSQueryHelper.addRelationTypeTripleToQuery(predName, predURI, semossQuery);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(predHash.get("SubjectVar"), predName, predHash.get("ObjectVar"), semossQuery);
		}
	}

	@Override
	public void buildQuery() 
	{
		semossQuery.setQueryType(SPARQLConstants.CONSTRUCT);
		semossQuery.setDisctinct(false);
		semossQuery.setReturnTripleArray(builderData.getRelTriples());
		parsePath();
		// we are assuming properties are passed in now based on user selection
//		parsePropertiesFromPath(); 
		configureQuery();
	}

	@Override
	protected void addReturnVariables(List<Hashtable<String, String>> predV2) {
		// TODO Auto-generated method stub
		
	}
}
