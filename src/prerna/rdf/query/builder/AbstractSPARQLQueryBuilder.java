package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.internal.StringMap;

import prerna.rdf.query.util.SEMOSSQuery;
import prerna.rdf.query.util.SEMOSSQueryHelper;
import prerna.rdf.query.util.SPARQLConstants;
import prerna.rdf.query.util.TriplePart;
import prerna.rdf.query.util.TriplePartConstant;

public abstract class AbstractSPARQLQueryBuilder extends AbstractQueryBuilder{

	SEMOSSQuery semossQuery = new SEMOSSQuery();
	static final String relVarArrayKey = "relVarTriples";
	List<String> totalVarList = new ArrayList<String>();
	List<Hashtable<String,String>> nodeV = new ArrayList<Hashtable<String,String>>();
	List<Hashtable<String,String>> predV = new ArrayList<Hashtable<String,String>>();
	List<Hashtable<String,String>> nodePropV = new ArrayList<Hashtable<String,String>>();
	Hashtable<String, List<Object>> filterDataHash = new Hashtable<String, List<Object>>();
	Hashtable<String, List<Object>> bindingsDataHash = new Hashtable<String, List<Object>>();
	Hashtable<String, Object> bindDataHash = new Hashtable<String, Object>();
	Hashtable<String, SEMOSSQuery> headerFilterHash = new Hashtable<String, SEMOSSQuery>();
	static final Logger logger = LogManager.getLogger(AbstractSPARQLQueryBuilder.class.getName());
	HashMap<String,Boolean> clearFilter = new HashMap();
	HashMap<String,ArrayList<Object>> searchFilter = new HashMap();
	
	protected void parsePath(){
		Hashtable<String, List> parsedPath = QueryBuilderHelper.parsePath(allJSONHash);
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
	
	protected void addNodesToQuery (List<Hashtable<String,String>> nodeV) {
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
	
	protected void addPropsToQuery (List<Hashtable<String,String>> nodePropV) {
		List<Hashtable<String, String>> propV = new ArrayList<Hashtable<String, String>>();
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

		searchFilterData();

		buildFilter();
		
		clearFilterData();
		
		// use nodeV, predV, and propV (as determined from parsing path and properties) to add necessary pieces to query
		// the rule is that all path variables must be included (otherwise we have a disconnected query) but properties are optional
		// optional part of properties already taken care of in parseProperties--here we only have props we want to add
		addNodesToQuery(nodeV);
		
		addRelationshipTriples(predV);
		
		addReturnVariables(predV);

		addPropsToQuery(nodePropV);
		
		addFilter();
	};
	
	abstract protected void addRelationshipTriples(List<Hashtable<String, String>> predV2);

	abstract protected void addReturnVariables(List<Hashtable<String, String>> predV2);

	
	private SEMOSSQuery createDefaultFilterQuery(String varName){
		SEMOSSQuery q = new SEMOSSQuery();
		q.setQueryType(SPARQLConstants.SELECT);
		q.setDisctinct(true);
		SEMOSSQueryHelper.addSingleReturnVarToQuery(varName, q);
		return q;
	}

	protected void searchFilterData(){
		StringMap<String> searchFilterResults = (StringMap<String>) allJSONHash.get(searchFilterKey);
		if(searchFilterResults != null){
			Iterator <String> keys = searchFilterResults.keySet().iterator();
			for(int colIndex = 0;keys.hasNext();colIndex++) // process one column at a time. At this point my key is title on the above
			{
				String varName = keys.next(); // this gets me title above
			
				String filterValue = searchFilterResults.get(varName);
				ArrayList<Object> filterValueArr = new ArrayList();
				filterValueArr.add(filterValue);
				if(filterValue.length()>0 ){
					searchFilter.put(varName,filterValueArr);
				}

			}
		
		}
	}
	protected void clearFilterData(){
		StringMap<String> clearFilterResults = (StringMap<String>) allJSONHash.get(clearFilterKey);
		if(clearFilterResults != null){
			Iterator <String> keys = clearFilterResults.keySet().iterator();
			for(int colIndex = 0;keys.hasNext();colIndex++) // process one column at a time. At this point my key is title on the above
			{
				String varName = keys.next(); // this gets me title above
				// need to split when there are underscores
				// for now keeping it simple

				// get the list
				String clearValues = clearFilterResults.get(varName);
				if(clearValues.length()>0 ){
					Boolean clearFilterValue = Boolean.valueOf(clearValues);
					clearFilter.put(varName,clearFilterValue);
				}

			}
		
		}
	}
	
	protected void buildFilter()
	{
		StringMap<List<Object>> filterResults = (StringMap<List<Object>>) allJSONHash.get(filterKey);
		if(filterResults != null)
		{
			for(String varName : filterResults.keySet())
			{
				List<Object> results = filterResults.get(varName);
				if(!results.isEmpty())
				{
					if(results.size() > 1) {
						if(bindingsDataHash.isEmpty()) {
							bindingsDataHash.put(varName, results);
						} else {
							Set<String> keySet = new HashSet<String>();
							keySet.addAll(bindingsDataHash.keySet());
							for(String previousVarName : keySet) {
								List<Object> previousResults = bindingsDataHash.get(previousVarName);
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
						Object bindValue = results.get(0);
						bindDataHash.put(varName, bindValue);
					}
				}
			}
		}
		
		if(!searchFilter.isEmpty()){
			for(String key: searchFilter.keySet()){
				if(bindDataHash.containsKey(key)){
					bindDataHash.remove(key);
				}
				if(bindingsDataHash.containsKey(key)){
					bindingsDataHash.remove(key);
				}
			}
		}
	}
	
	protected void addFilter(){
		// add filtering
		boolean caseSensitiveFilter = true;
		for(String s : bindDataHash.keySet())
		{
			Object bindValue = bindDataHash.get(s);
			TriplePartConstant triplePartC;
			if(bindValue.toString().startsWith("http")) {
				bindValue = bindValue.toString();
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
			List<Object> bindingValues = bindingsDataHash.get(s);
			String bindValue = bindingValues.get(0).toString();
			// if value is empty, loop through until we find a value that is not empty to determine the type
			if(bindValue.isEmpty()) {
				int i = 1;
				while(i < bindingValues.size()) {
					bindValue = bindingValues.get(i).toString();
					if(!bindValue.isEmpty()) {
						break;
					}
				}
			}
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
			SEMOSSQueryHelper.addBindingsToQuery(bindingValues, triplePartC, s.toString(), this.semossQuery);
		}
		
		for(String s : filterDataHash.keySet())
		{
			List<Object> filterOptions = filterDataHash.get(s);
//			// for every filter query, add the bind as long as it is not the variable in question
//			for(String varName : this.headerFilterHash.keySet()){
//				if(!s.equals(varName)){
//					SEMOSSQuery q = this.headerFilterHash.get(varName);
//					SEMOSSQueryHelper.addRegexFilterPhrase(s, TriplePart.VARIABLE, filterOptions, TriplePart.LITERAL, false, true, q);
//				}
//			}
			String filterValue = filterOptions.get(0).toString();
			// if value is empty, loop through until we find a value that is not empty to determine the type
			if(filterValue.isEmpty()) {
				int i = 1;
				while(i < filterOptions.size()) {
					filterValue = filterOptions.get(i).toString();
					if(!filterValue.isEmpty()) {
						break;
					}
				}
			}
			TriplePartConstant triplePartC;
			if(filterValue.startsWith("http")) {
				triplePartC = TriplePart.URI;
				SEMOSSQueryHelper.addURIFilterPhrase(s, TriplePart.VARIABLE, filterOptions, triplePartC, true, semossQuery);
			} else {
				triplePartC = TriplePart.LITERAL;
				caseSensitiveFilter = true;
				SEMOSSQueryHelper.addRegexFilterPhrase(s, TriplePart.VARIABLE, filterOptions, triplePartC, false, true, semossQuery, caseSensitiveFilter);
			}
		}
		for(String s: searchFilter.keySet()){
			// since URIs are defaulted as case sensitive, we will treat URIs are string literals
			caseSensitiveFilter = false;
			ArrayList<Object> filterOptions = searchFilter.get(s);
			SEMOSSQueryHelper.addRegexFilterPhrase(s, TriplePart.VARIABLE, filterOptions, TriplePart.LITERAL, false, true, semossQuery, caseSensitiveFilter);
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
			
			boolean clearFilterResults = false;
			if(clearFilter.size()>0 && clearFilter.containsKey(varName)){
				if(clearFilter.get(varName)){
					clearFilterResults = true;
				}
			}
			
			if(clearFilterResults && this.headerFilterHash.containsKey(varName)){
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
	
	public List<Hashtable<String, String>> getNodeV(){
		return this.nodeV;
	}
	
	public List<Hashtable<String, String>> getPredV(){
		return this.predV;
	}
	
	public List<Hashtable<String, String>> getNodePropV(){
		return this.nodePropV;
	}
	
	@Override
	public String getQuery() {
		semossQuery.createQuery();
		return semossQuery.getQuery();
	}
	
	@Override
	public SEMOSSQuery getSEMOSSQuery(){
		return this.semossQuery;
	}
}
