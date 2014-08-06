package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;
import prerna.rdf.query.util.SPARQLConstants;
import prerna.rdf.query.util.TriplePart;
import prerna.rdf.query.util.TriplePartConstant;
import prerna.util.Utility;

import com.google.gson.internal.StringMap;

public class CustomVizTableBuilder extends AbstractCustomVizBuilder{

	static final String relArrayKey = "relTriples";
	static final String relVarArrayKey = "relVarTriples";
	static final String filterKey = "filter";
	ArrayList<String> totalVarList = new ArrayList<String>();
	ArrayList<Hashtable<String,String>> nodeV = new ArrayList<Hashtable<String,String>>();
	ArrayList<Hashtable<String,String>> predV = new ArrayList<Hashtable<String,String>>();
	ArrayList<Hashtable<String,String>> nodePropV = new ArrayList<Hashtable<String,String>>();
	ArrayList<Hashtable<String,String>> edgePropV = new ArrayList<Hashtable<String,String>>();
	Hashtable<String, ArrayList<Object>> filterDataHash = new Hashtable<String, ArrayList<Object>>();
	Hashtable<String, ArrayList<Object>> bindingsDataHash = new Hashtable<String, ArrayList<Object>>();
	Hashtable<String, Object> bindDataHash = new Hashtable<String, Object>();
	Hashtable<String, SEMOSSQuery> headerFilterHash = new Hashtable<String, SEMOSSQuery>();
	IEngine coreEngine = null;
	static final int subIdx = 0;
	static final int predIdx = 1;
	static final int objIdx = 2;
	static final int propIdx = 0;
	static final String uriKey = "uriKey";
	static final String queryKey = "queryKey";
	static final String varKey = "varKey";
	static final Logger logger = LogManager.getLogger(CustomVizTableBuilder.class.getName());


	@Override
	public void buildQuery() 
	{
		semossQuery.setQueryType(SPARQLConstants.SELECT);
		semossQuery.setDisctinct(true);
		parsePath();
		// we are assuming properties are passed in now based on user selection
//		parsePropertiesFromPath(); 
		configureQuery();
		semossQuery.createQuery();		
	}

	public Hashtable<String, ArrayList<Hashtable<String,String>>> getPropsFromPath() 
	{
		parsePath();
		parsePropertiesFromPath();	
		Hashtable<String, ArrayList<Hashtable<String,String>>> propsHash = new Hashtable<String, ArrayList<Hashtable<String,String>>>();
		propsHash.put("nodes", nodePropV);
		propsHash.put("edges", edgePropV);
		return propsHash;
	}
	
	private void configureQuery(){

		buildFilter();
		ArrayList<SEMOSSQuery> filterQueries = new ArrayList(this.headerFilterHash.values());
		filterQueries.add(this.semossQuery);
		
		// use nodeV, predV, and propV (as determined from parsing path and properties) to add necessary pieces to query
		// the rule is that all path variables must be included (otherwise we have a disconnected query) but properties are optional
		// optional part of properties already taken care of in parseProperties--here we only have props we want to add
		for(Hashtable<String, String> nodeHash : nodeV){
			String nodeName = nodeHash.get(varKey);
			String nodeURI = nodeHash.get(uriKey);
			for(SEMOSSQuery filterQuery : filterQueries){
				SEMOSSQueryHelper.addConceptTypeTripleToQuery(nodeName, nodeURI, filterQuery);
			}
			SEMOSSQueryHelper.addSingleReturnVarToQuery(nodeName, semossQuery);
		}

		for(Hashtable<String, String> predHash : predV){
			String predName = predHash.get(varKey);
			String predURI = predHash.get(uriKey);
			for(SEMOSSQuery filterQuery : filterQueries){
				SEMOSSQueryHelper.addRelationTypeTripleToQuery(predName, predURI, filterQuery);
				SEMOSSQueryHelper.addRelationshipVarTripleToQuery(predHash.get("SubjectVar"), predName, predHash.get("ObjectVar"),filterQuery);
			}
		}

		ArrayList<Hashtable<String, String>> propV = new ArrayList<Hashtable<String, String>>();
		propV.addAll(nodePropV);
		propV.addAll(edgePropV);
		for(Hashtable<String, String> propHash : propV) {
			String propName = propHash.get(varKey);
			String propURI = propHash.get(uriKey);
			for(SEMOSSQuery filterQuery : filterQueries){
				SEMOSSQueryHelper.addGenericTriple(propHash.get("SubjectVar"), TriplePart.VARIABLE, propURI, TriplePart.URI, propName, TriplePart.VARIABLE, filterQuery);
			}
			SEMOSSQueryHelper.addSingleReturnVarToQuery(propName, semossQuery);
		}
		
		addFilter();
	}

	private void parsePath()
	{
		ArrayList<ArrayList<String>> tripleArray = (ArrayList<ArrayList<String>>) allJSONHash.get(relArrayKey);
		for (int tripleIdx = 0; tripleIdx<tripleArray.size(); tripleIdx++)
		{
			ArrayList<String> thisTripleArray = tripleArray.get(tripleIdx);
			String subjectURI = thisTripleArray.get(subIdx);
			String subjectName = Utility.getInstanceName(subjectURI);
			// store node/rel info
			if (!totalVarList.contains(subjectName))
			{
				//store node info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put(varKey, subjectName);
				elementHash.put(uriKey, subjectURI);
				totalVarList.add(subjectName);
				nodeV.add(elementHash);
			}
			// if a full path has been selected and not just a single node, go through predicate and object
			if(thisTripleArray.size()>1)
			{
				String predURI = thisTripleArray.get(predIdx);
				String objectURI = thisTripleArray.get(objIdx);
				String objectName = Utility.getInstanceName(objectURI);
				String predName = subjectName + "_" +Utility.getInstanceName(predURI) + "_" + objectName;
				if (!totalVarList.contains(predName))
				{
					Hashtable<String, String> predInfoHash = new Hashtable<String,String>();
					predInfoHash.put("Subject", subjectURI);
					predInfoHash.put("SubjectVar", subjectName);
					predInfoHash.put("Pred", predURI);
					predInfoHash.put("Object", objectURI);
					predInfoHash.put("ObjectVar", objectName);
					predInfoHash.put(uriKey, predURI);
					predInfoHash.put(varKey, predName);

					totalVarList.add(predName);
					predV.add(predInfoHash);
				}
				if (!totalVarList.contains(objectName))
				{
					totalVarList.add(objectName);
					//store node info
					Hashtable<String, String> elementHash = new Hashtable<String, String>();
					elementHash.put(varKey, objectName);
					elementHash.put(uriKey, objectURI);
					nodeV.add(elementHash);
				}
			}
		}
	}
	
	private SEMOSSQuery createDefaultFilterQuery(String varName){
		SEMOSSQuery q = new SEMOSSQuery();
		q.setQueryType(SPARQLConstants.SELECT);
		q.setDisctinct(true);
		SEMOSSQueryHelper.addSingleReturnVarToQuery(varName, q);
		return q;
	}

	private void buildFilter()
	{
		StringMap<ArrayList<Object>> filterResults = (StringMap<ArrayList<Object>>) allJSONHash.get(filterKey);
		if(filterResults != null)
		{
			for(String varName : filterResults.keySet())
			{
				this.headerFilterHash.put(varName, createDefaultFilterQuery(varName));
				ArrayList<Object> results = filterResults.get(varName);
				if(!results.isEmpty())
				{
					if(results.size() > 1) {
						if(bindingsDataHash.isEmpty()) {
							bindingsDataHash.put(varName, results);
						} else {
							for(String previousVarName : bindingsDataHash.keySet()) {
								ArrayList<Object> previousResults = bindingsDataHash.get(previousVarName);
								if(results.size() > previousResults.size()) {
									filterDataHash.put(previousVarName, previousResults);
									bindingsDataHash.clear();
									bindingsDataHash.put(varName, results);
								} else {
									filterDataHash.put(varName, results);
								}
							}
						}
					} else {
						// this means there is only 1 element so we use bind for sparql efficiency
						String bindValue = (String) results.get(0);
						bindDataHash.put(varName, bindValue);
					}
				}
			}
		}
	}
	
	private void addFilter(){
		// add filtering
		for(String s : bindDataHash.keySet())
		{
			String bindValue = bindDataHash.get(s).toString();
			TriplePartConstant triplePartC;
			if(bindValue.startsWith("http")) {
				triplePartC = TriplePart.URI;
			} else {
				triplePartC = TriplePart.LITERAL;
			}
			// for every filter query, add the bind as long as it is not the variable in question
			for(String varName : this.headerFilterHash.keySet()){
				if(!s.equals(varName)){
					SEMOSSQuery q = this.headerFilterHash.get(varName);
					SEMOSSQueryHelper.addBindPhrase(bindValue, triplePartC, s, q);
				}
			}
			SEMOSSQueryHelper.addBindPhrase(bindValue, triplePartC, s, semossQuery);
		}
		for(String s : bindingsDataHash.keySet())
		{
			String bindValue = bindingsDataHash.get(s).get(0).toString();
			// TODO: find better logic to determine if dealing with URI or Literal
			TriplePartConstant triplePartC;
			if(bindValue.startsWith("http")) {
				triplePartC = TriplePart.URI;
			} else {
				triplePartC = TriplePart.LITERAL;
			}
			// for every filter query, add the bind as long as it is not the variable in question
			for(String varName : this.headerFilterHash.keySet()){
				if(!s.equals(varName)){
					SEMOSSQuery q = this.headerFilterHash.get(varName);
					SEMOSSQueryHelper.addBindingsToQuery(bindingsDataHash.get(s), triplePartC, s.toString(), q);
				}
			}
			SEMOSSQueryHelper.addBindingsToQuery(bindingsDataHash.get(s), triplePartC, s.toString(), this.semossQuery);
		}
		for(String s : filterDataHash.keySet())
		{
			ArrayList<Object> filterOptions = filterDataHash.get(s);
			// for every filter query, add the bind as long as it is not the variable in question
			for(String varName : this.headerFilterHash.keySet()){
				if(!s.equals(varName)){
					SEMOSSQuery q = this.headerFilterHash.get(varName);
					SEMOSSQueryHelper.addRegexFilterPhrase(s, TriplePart.VARIABLE, filterOptions, TriplePart.LITERAL, false, true, q);
				}
			}
			SEMOSSQueryHelper.addRegexFilterPhrase(s, TriplePart.VARIABLE, filterOptions, TriplePart.LITERAL, false, true, semossQuery);
		}
	}

	private void parsePropertiesFromPath()
	{
		ArrayList<Hashtable<String, String>> relationVarList = new ArrayList<Hashtable<String, String>>();
		relationVarList.addAll(nodeV);
		// run through all of the variables to get properties
		for (int i=0;i<relationVarList.size();i++)
		{
			Hashtable<String, String> vHash = relationVarList.get(i);
			String varName = vHash.get(varKey);
			String varURI = vHash.get(uriKey);
			String nodePropQuery = "SELECT DISTINCT ?entity WHERE {{?source <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+varURI+">} {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?source ?entity ?prop }}";
			Vector<String> propVector = coreEngine.getEntityOfType(nodePropQuery);
			for (int propIdx=0 ; propIdx<propVector.size(); propIdx++)
			{
				String propURI = propVector.get(propIdx);
				String propName = varName + "__" + Utility.getInstanceName(propURI).replace("-",  "_");
				totalVarList.add(propName);
				//store node prop info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put("SubjectVar", varName);
				elementHash.put(varKey, propName);
				elementHash.put(uriKey, propURI);
				nodePropV.add(elementHash);
			}
		}
		ArrayList<Hashtable<String, String>> predVarList = new ArrayList<Hashtable<String, String>>();
		predVarList.addAll(predV);
		// run through all of the predicates (which aren't variables) to get properties
		for (int i=0;i<predVarList.size();i++)
		{
			Hashtable<String, String> predInfoHash = predVarList.get(i);
			String varName = predInfoHash.get(varKey);

			String edgePropQuery = "SELECT DISTINCT ?entity WHERE {{?source <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + predInfoHash.get("Subject") + ">} {?target <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + predInfoHash.get("Object") + "> } {?verb <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <" + predInfoHash.get("Pred") + "> }{?source ?verb ?target;} {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?verb ?entity ?prop }}";
			Vector<String> propVector = coreEngine.getEntityOfType(edgePropQuery);
			for (int propIdx=0 ; propIdx<propVector.size(); propIdx++)
			{
				String propURI = propVector.get(propIdx);
				String propName = varName + "__" + Utility.getInstanceName(propURI).replace("-",  "_");
				totalVarList.add(propName);
				//store rel prop info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put("SubjectVar", varName);
				elementHash.put(varKey, propName);
				elementHash.put(uriKey, propURI);
				edgePropV.add(elementHash);
			}
		}
	}

	public void setEngine(IEngine coreEngine)
	{
		this.coreEngine = coreEngine;
	}
	
	public void setPropV(ArrayList<Hashtable<String,String>> selectedNodePropsList, ArrayList<Hashtable<String,String>> selectedEdgePropsList)
	{
		this.nodePropV = selectedNodePropsList;
		this.edgePropV = selectedEdgePropsList;
	}
	
	public String getQueryPattern(){
		return this.semossQuery.getQueryPattern();
	}
	
	public Integer runCountQuery(){
		String q = this.semossQuery.getCountQuery();
		logger.info("Count query generated : " + q);
		Vector<String> countV = coreEngine.getEntityOfType(q);
		int totalSize = 0;
		if(countV.size()>0)
		{
			System.out.println(countV.get(0));
			try 
			{
				totalSize = Integer.parseInt(countV.get(0));
			}catch (NumberFormatException e){
				logger.error(e);
			}
		}
		return totalSize;
	}

	public ArrayList<Hashtable<String,String>> getHeaderArray(){
		ArrayList<Hashtable<String,String>> retArray = new ArrayList<Hashtable<String,String>>();
		retArray.addAll(nodeV);
		retArray.addAll(nodePropV);
		retArray.addAll(edgePropV);
		// add the filter queries
		String defaultQueryPattern = this.semossQuery.getQueryPattern();
		for(Hashtable<String, String> headerHash : retArray){
			String varName = headerHash.get(this.varKey);
			String filterQuery = "";
			if(this.headerFilterHash.containsKey(varName)){
				SEMOSSQuery q = headerFilterHash.get(varName);
				q.createQuery();
				filterQuery = q.getQuery();
			}
			else{
				filterQuery = "SELECT DISTINCT ?" + varName + " " + defaultQueryPattern;
			}
			headerHash.put(this.queryKey, filterQuery);
		}
		return retArray;
	}
}
