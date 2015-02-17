package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;
import prerna.rdf.query.util.SPARQLConstants;
import prerna.rdf.query.util.TriplePart;
import prerna.rdf.query.util.TriplePartConstant;

import com.google.gson.Gson;
import com.google.gson.internal.StringMap;
import com.google.gson.reflect.TypeToken;

public abstract class AbstractSPARQLQueryBuilder extends AbstractQueryBuilder{

	SEMOSSQuery semossQuery = new SEMOSSQuery();
	static final String relVarArrayKey = "relVarTriples";
	static final String filterKey = "filter";
	ArrayList<String> totalVarList = new ArrayList<String>();
	ArrayList<Hashtable<String,String>> nodeV = new ArrayList<Hashtable<String,String>>();
	ArrayList<Hashtable<String,String>> predV = new ArrayList<Hashtable<String,String>>();
	ArrayList<Hashtable<String,String>> nodePropV = new ArrayList<Hashtable<String,String>>();
	Hashtable<String, ArrayList<Object>> filterDataHash = new Hashtable<String, ArrayList<Object>>();
	Hashtable<String, ArrayList<Object>> bindingsDataHash = new Hashtable<String, ArrayList<Object>>();
	Hashtable<String, Object> bindDataHash = new Hashtable<String, Object>();
	Hashtable<String, SEMOSSQuery> headerFilterHash = new Hashtable<String, SEMOSSQuery>();
	static final Logger logger = LogManager.getLogger(AbstractSPARQLQueryBuilder.class.getName());
	
	protected void parsePath(){
		Hashtable<String, ArrayList> parsedPath = QueryBuilderHelper.parsePath(allJSONHash);
		totalVarList = parsedPath.get(QueryBuilderHelper.totalVarListKey);
		nodeV = parsedPath.get(QueryBuilderHelper.nodeVKey);
		predV = parsedPath.get(QueryBuilderHelper.predVKey);
	}
	
	@Override
	public void setJSONDataHash(Hashtable<String, Object> allJSONHash) {
		Gson gson = new Gson();
		this.allJSONHash = new Hashtable<String, Object>();
		this.allJSONHash.putAll((StringMap) allJSONHash.get("QueryData"));
		ArrayList<StringMap> list = (ArrayList<StringMap>) allJSONHash.get("SelectedNodeProps") ;
		this.nodePropV = new ArrayList<Hashtable<String, String>>();
		for(StringMap map : list){
			Hashtable hash = new Hashtable();
			hash.putAll(map);
			nodePropV.add(hash);
		}
	}
	
	protected void addNodesToQuery (ArrayList<Hashtable<String,String>> nodeV) {
		for(Hashtable<String, String> nodeHash : nodeV){
			String nodeName = nodeHash.get(QueryBuilderHelper.varKey);
			String nodeURI = nodeHash.get(QueryBuilderHelper.uriKey);
			SEMOSSQuery headerQuery = createDefaultFilterQuery(nodeName);
			this.headerFilterHash.put(nodeName, headerQuery);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(nodeName, nodeURI, headerQuery);
			
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(nodeName, nodeURI, semossQuery);
			SEMOSSQueryHelper.addSingleReturnVarToQuery(nodeName, semossQuery);
		}
	}
	
	protected void addPropsToQuery (ArrayList<Hashtable<String,String>> nodePropV) {
		ArrayList<Hashtable<String, String>> propV = new ArrayList<Hashtable<String, String>>();
		propV.addAll(nodePropV);
		for(Hashtable<String, String> propHash : propV) {
			String propName = propHash.get(QueryBuilderHelper.varKey);
			String propURI = propHash.get(QueryBuilderHelper.uriKey);
			SEMOSSQueryHelper.addGenericTriple(propHash.get("SubjectVar"), TriplePart.VARIABLE, propURI, TriplePart.URI, propName, TriplePart.VARIABLE, semossQuery);
			SEMOSSQueryHelper.addSingleReturnVarToQuery(propName, semossQuery);
		}
	}
	
	protected void configureQuery(){ 
		// TODO Auto-generated method stub

		buildFilter();
		
		// use nodeV, predV, and propV (as determined from parsing path and properties) to add necessary pieces to query
		// the rule is that all path variables must be included (otherwise we have a disconnected query) but properties are optional
		// optional part of properties already taken care of in parseProperties--here we only have props we want to add
		addNodesToQuery(nodeV);
		
		addRelationshipTriples(predV);
		
		addReturnVariables(predV);

		addPropsToQuery(nodePropV);
		
		addFilter();
	};
	
	abstract protected void addRelationshipTriples(
			ArrayList<Hashtable<String, String>> predV2);

	abstract protected void addReturnVariables(ArrayList<Hashtable<String, String>> predV2);

	
	private SEMOSSQuery createDefaultFilterQuery(String varName){
		SEMOSSQuery q = new SEMOSSQuery();
		q.setQueryType(SPARQLConstants.SELECT);
		q.setDisctinct(true);
		SEMOSSQueryHelper.addSingleReturnVarToQuery(varName, q);
		return q;
	}

	protected void buildFilter()
	{
		StringMap<ArrayList<Object>> filterResults = (StringMap<ArrayList<Object>>) allJSONHash.get(filterKey);
		if(filterResults != null)
		{
			for(String varName : filterResults.keySet())
			{
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
	
	protected void addFilter(){
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
//			// for every filter query, add the bind as long as it is not the variable in question
//			for(String varName : this.headerFilterHash.keySet()){
//				if(!s.equals(varName)){
//					SEMOSSQuery q = this.headerFilterHash.get(varName);
//					SEMOSSQueryHelper.addBindPhrase(bindValue, triplePartC, s, q);
//				}
//			}
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
//			// for every filter query, add the bind as long as it is not the variable in question
//			for(String varName : this.headerFilterHash.keySet()){
//				if(!s.equals(varName)){
//					SEMOSSQuery q = this.headerFilterHash.get(varName);
//					SEMOSSQueryHelper.addBindingsToQuery(bindingsDataHash.get(s), triplePartC, s.toString(), q);
//				}
//			}
			SEMOSSQueryHelper.addBindingsToQuery(bindingsDataHash.get(s), triplePartC, s.toString(), this.semossQuery);
		}
		for(String s : filterDataHash.keySet())
		{
			ArrayList<Object> filterOptions = filterDataHash.get(s);
//			// for every filter query, add the bind as long as it is not the variable in question
//			for(String varName : this.headerFilterHash.keySet()){
//				if(!s.equals(varName)){
//					SEMOSSQuery q = this.headerFilterHash.get(varName);
//					SEMOSSQueryHelper.addRegexFilterPhrase(s, TriplePart.VARIABLE, filterOptions, TriplePart.LITERAL, false, true, q);
//				}
//			}
			SEMOSSQueryHelper.addRegexFilterPhrase(s, TriplePart.VARIABLE, filterOptions, TriplePart.LITERAL, false, true, semossQuery);
		}
	}

	public void setPropV(ArrayList<Hashtable<String,String>> selectedNodePropsList)
	{
		this.nodePropV = selectedNodePropsList;
	}
			
	public ArrayList<Hashtable<String,String>> getHeaderArray(){
		ArrayList<Hashtable<String,String>> retArray = new ArrayList<Hashtable<String,String>>();
		retArray.addAll(nodeV);
		retArray.addAll(nodePropV);
		// add the filter queries
		String defaultQueryPattern = this.semossQuery.getQueryPattern();
		for(Hashtable<String, String> headerHash : retArray){
			String varName = headerHash.get(QueryBuilderHelper.varKey);
			String filterQuery = "";
			if(this.headerFilterHash.containsKey(varName)){
				SEMOSSQuery q = headerFilterHash.get(varName);
				q.createQuery();
				filterQuery = q.getQuery();
			}
			else{
				filterQuery = "SELECT DISTINCT ?" + varName + " " + defaultQueryPattern ;
			}
			headerHash.put(QueryBuilderHelper.queryKey, filterQuery);
		}
		return retArray;
	}
	
	public ArrayList<Hashtable<String, String>> getNodeV(){
		return this.nodeV;
	}
	
	public ArrayList<Hashtable<String, String>> getPredV(){
		return this.predV;
	}

	@Override
	public String getQuery() {
		semossQuery.createQuery();
		return semossQuery.getQuery();
	}
	
	public SEMOSSQuery getSEMOSSQuery(){
		return this.semossQuery;
	}
}
