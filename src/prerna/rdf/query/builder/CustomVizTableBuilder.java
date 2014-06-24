package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.query.util.SEMOSSQueryHelper;
import prerna.rdf.query.util.SPARQLConstants;
import prerna.rdf.query.util.TriplePart;
import prerna.util.Utility;

import com.google.gson.internal.StringMap;

public class CustomVizTableBuilder extends AbstractCustomVizBuilder{

	static final String relArrayKey = "relTriples";
	static final String relVarArrayKey = "relVarTriples";
	static final String filterKey = "filter";
	ArrayList<String> varList = new ArrayList<String>();
	Hashtable<String, Hashtable<String,String>> varObjHash = new Hashtable<String, Hashtable<String,String>>();
	Hashtable<String, Hashtable<String,String>> predHash = new Hashtable<String, Hashtable<String,String>>();
	Hashtable<String, ArrayList<Object>> filterDataHash = new Hashtable<String, ArrayList<Object>>();
	Hashtable<String, ArrayList<Object>> bindingsDataHash = new Hashtable<String, ArrayList<Object>>();
	Hashtable<String, Object> bindDataHash = new Hashtable<String, Object>();
	String bindString = "";
	String filterString = "";
	String bindingsString = "";
	IEngine coreEngine = null;
	static final int subIdx = 0;
	static final int predIdx = 1;
	static final int objIdx = 2;
	static final int propIdx = 0;
	static final String uriKey = "uriKey";
	static final String queryKey = "queryKey";
	static final String varKey = "varKey";
	String nodeInstanceQuery = "SELECT DISTINCT ?@INSTANCE@ WHERE {{?@INSTANCE@ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@NODE_TYPE@>} @BIND@ @FILTER@ } @BINDINGS@";
	String relInstanceQuery = "SELECT DISTINCT ?@INSTANCE@ WHERE {{?inNode <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@IN_NODE_TYPE@>} {?outNode <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@OUT_NODE_TYPE@>}{?@INSTANCE@ <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <@REL_TYPE@>} {?inNode ?@INSTANCE@ ?outNode} @BIND@ @FILTER@ } @BINDINGS@";
	String nodePropQuery = "SELECT DISTINCT ?@INSTANCE@ WHERE {{?@NODE@ <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@NODE_TYPE@>} {?@NODE@ <@PROP_TYPE@> ?@INSTANCE@ } @BIND@ @FILTER@ } @BINDINGS@";
	String relPropQuery = "SELECT DISTINCT ?@INSTANCE@ WHERE {{?inNode <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@IN_NODE_TYPE@>} {?outNode <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@OUT_NODE_TYPE@>}{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <@REL_TYPE@>} {?inNode ?rel ?outNode} {?rel <@PROP_TYPE@> ?@INSTANCE@} @BIND@ @FILTER@ } @BINDINGS@";
	Logger logger = Logger.getLogger(getClass());


	@Override
	public void buildQuery() 
	{
		semossQuery.setQueryType(SPARQLConstants.SELECT);
		semossQuery.setDisctinct(true);
		buildFilter();
		buildQueryForSelectedPath();
		buildQueryForCount();
		semossQuery.createQuery();		
	}

	private void buildFilter()
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

	private void buildQueryForSelectedPath()
	{
		ArrayList<ArrayList<String>> tripleArray = (ArrayList<ArrayList<String>>) allJSONHash.get(relArrayKey);
		for (int tripleIdx = 0; tripleIdx<tripleArray.size(); tripleIdx++)
		{
			String subjectURI = tripleArray.get(tripleIdx).get(subIdx);
			String predURI = tripleArray.get(tripleIdx).get(predIdx);
			String objectURI = tripleArray.get(tripleIdx).get(objIdx);
			String subjectName = Utility.getInstanceName(subjectURI);
			String objectName = Utility.getInstanceName(objectURI);
			String predName = subjectName + "_" +Utility.getInstanceName(predURI) + "_" + objectName;
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(subjectName, subjectURI, semossQuery);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(objectName, objectURI, semossQuery);
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(predName, predURI, semossQuery);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(subjectName, predName, objectName,semossQuery);
			// store node/rel info
			if (!varList.contains(subjectName))
			{
				varList.add(subjectName);
				//store node info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put(varKey, subjectName);
				elementHash.put(uriKey, subjectURI);
				varObjHash.put(subjectName, elementHash);
			}
			if (!varList.contains(predName))
			{
				Hashtable<String, String> predInfoHAsh = new Hashtable<String,String>();
				predInfoHAsh.put("Subject", subjectURI);
				predInfoHAsh.put("Pred", predURI);
				predInfoHAsh.put("Object", objectURI);
				varList.add(predName);
				//store rel info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put(varKey, predName);
				elementHash.put(uriKey, predURI);
				varObjHash.put(predName, elementHash);

				predHash.put(predName,  predInfoHAsh);
			}
			if (!varList.contains(objectName))
			{
				varList.add(objectName);
				//store node info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put(varKey, objectName);
				elementHash.put(uriKey, objectURI);
				varObjHash.put(objectName, elementHash);
			}
		}
		// add return variables
		for(int i=0;i<varList.size();i++)
		{
			SEMOSSQueryHelper.addSingleReturnVarToQuery(varList.get(i), semossQuery);
		}
		// add filtering
		for(String s : bindDataHash.keySet())
		{
			String bindValue = bindDataHash.get(s).toString();
			if(bindValue.startsWith("http")) {
				SEMOSSQueryHelper.addBindPhrase(bindValue, TriplePart.URI, s, semossQuery);
			} else {
				SEMOSSQueryHelper.addBindPhrase(bindValue, TriplePart.LITERAL, s, semossQuery);
			}
		}
		for(String s : bindingsDataHash.keySet())
		{
			String bindValue = bindingsDataHash.get(s).get(0).toString();
			// TODO: find better logic to determine if dealing with URI or Literal
			if(bindValue.startsWith("http")) {
				SEMOSSQueryHelper.addBindingsToQuery(bindingsDataHash.get(s), TriplePart.URI, s.toString(), semossQuery);
			} else {
				SEMOSSQueryHelper.addBindingsToQuery(bindingsDataHash.get(s), TriplePart.LITERAL, s.toString(), semossQuery);
			}
		}
		for(String s : filterDataHash.keySet())
		{
			ArrayList<Object> filterOptions = filterDataHash.get(s);
			SEMOSSQueryHelper.addRegexFilterPhrase(s, TriplePart.VARIABLE, filterOptions, TriplePart.LITERAL, false, true, semossQuery);
		}
	}

	private void buildQueryForCount()
	{
		ArrayList<String> relationVarList = new ArrayList<String>();
		relationVarList.addAll(varList);
		for (int i=0;i<relationVarList.size();i++)
		{
			String varName = relationVarList.get(i);
			String varURI = varObjHash.get(varName).get(uriKey);
			String propName = "";
			String propURI = "";
			if (!predHash.containsKey(varName))
			{
				String nodePropQuery = "SELECT DISTINCT ?entity WHERE {{?source <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+varURI+">} {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?source ?entity ?prop }}";
				Vector<String> propV = coreEngine.getEntityOfType(nodePropQuery);
				for (int propIdx=0 ; propIdx<propV.size(); propIdx++)
				{
					propURI = propV.get(propIdx);
					propName = varName + "__" + Utility.getInstanceName(propURI).replace("-",  "_");
					SEMOSSQueryHelper.addGenericTriple(varName, TriplePart.VARIABLE, propURI, TriplePart.URI, propName, TriplePart.VARIABLE, semossQuery);
					SEMOSSQueryHelper.addSingleReturnVarToQuery(propName, semossQuery);
					varList.add(propName);
					//store node prop info
					Hashtable<String, String> elementHash = new Hashtable<String, String>();
					elementHash.put(varKey, propName);
					elementHash.put(uriKey, propURI);
					varObjHash.put(propName, elementHash);
				}
			}
			else
			{
				Hashtable<String, String> predInfoHash = predHash.get(varName);

				String edgePropQuery = "SELECT DISTINCT ?entity WHERE {{?source <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + predInfoHash.get("Subject") + ">} {?target <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + predInfoHash.get("Object") + "> } {?verb <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <" + predInfoHash.get("Pred") + "> }{?source ?verb ?target;} {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?verb ?entity ?prop }}";
				Vector<String> propV = coreEngine.getEntityOfType(edgePropQuery);
				for (int propIdx=0 ; propIdx<propV.size(); propIdx++)
				{
					propURI = propV.get(propIdx);
					propName = varName + "__" + Utility.getInstanceName(propURI).replace("-",  "_");
					SEMOSSQueryHelper.addGenericTriple(varName, TriplePart.VARIABLE, propURI, TriplePart.URI, propName, TriplePart.VARIABLE, semossQuery);
					SEMOSSQueryHelper.addSingleReturnVarToQuery(propName, semossQuery);
					varList.add(propName);
					//store rel prop info
					Hashtable<String, String> elementHash = new Hashtable<String, String>();
					elementHash.put(varKey, propName);
					elementHash.put(uriKey, propURI);
					varObjHash.put(propName, elementHash);
				}
			}
		}
	}

	public void setEngine(IEngine coreEngine)
	{
		this.coreEngine = coreEngine;
	}

	public Hashtable<String, Hashtable<String,String>> getVarObjHash(){
		return this.varObjHash;
	}
}
