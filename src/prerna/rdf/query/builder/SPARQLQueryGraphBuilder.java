package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.query.util.SEMOSSQueryHelper;
import prerna.rdf.query.util.SPARQLConstants;
import prerna.rdf.query.util.TriplePart;

public class SPARQLQueryGraphBuilder extends AbstractSPARQLQueryBuilder {
	static final Logger logger = LogManager.getLogger(SPARQLQueryGraphBuilder.class.getName());

	protected void addRelationshipTriples (ArrayList<Hashtable<String,String>> predV) {
		for(Hashtable<String, String> predHash : predV){
			String predName = predHash.get(varKey);
			String predURI = predHash.get(uriKey);

			SEMOSSQueryHelper.addRelationTypeTripleToQuery(predName, predURI, semossQuery);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(predHash.get("SubjectVar"), predName, predHash.get("ObjectVar"), semossQuery);
		}
	}

	@Override
	public void buildQuery() 
	{
		semossQuery.setQueryType(SPARQLConstants.CONSTRUCT);
		semossQuery.setDisctinct(false);
		semossQuery.setReturnTripleArray((ArrayList<ArrayList<String>>) allJSONHash.get(relArrayKey));
		parsePath();
		// we are assuming properties are passed in now based on user selection
//		parsePropertiesFromPath(); 
		configureQuery();
	}

	@Override
	protected void addReturnVariables(
			ArrayList<Hashtable<String, String>> predV2) {
		// TODO Auto-generated method stub
		
	}
}
