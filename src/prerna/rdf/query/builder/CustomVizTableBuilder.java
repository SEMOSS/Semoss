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

public class CustomVizTableBuilder extends AbstractCustomVizBuilder{

	static final String relArrayKey = "relTriples";
	static final String relVarArrayKey = "relVarTriples";
	ArrayList<String> varList = new ArrayList<String>();
	Hashtable<String, Hashtable<String,String>> varObjHash = new Hashtable<String, Hashtable<String,String>>();
	Hashtable<String, Hashtable<String,String>> predHash = new Hashtable<String, Hashtable<String,String>>();
	IEngine coreEngine = null;
	static final int subIdx = 0;
	static final int predIdx = 1;
	static final int objIdx = 2;
	static final int propIdx = 0;
	static final String uriKey = "uriKey";
	static final String queryKey = "queryKey";
	static final String varKey = "varKey";
	String nodeInstanceQuery = "SELECT DISTINCT ?instance WHERE {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@NODE_TYPE@>}";
	String relInstanceQuery = "SELECT DISTINCT ?instance WHERE {{?inNode <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@IN_NODE_TYPE@>} {?outNode <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@OUT_NODE_TYPE@>}{?instance <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <@REL_TYPE@>} {?inNode ?instance ?outNode}}";
	String nodePropQuery = "SELECT DISTINCT ?instance WHERE {{?node <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@NODE_TYPE@>} {?node <@PROP_TYPE@> ?instance }}";
	String relPropQuery = "SELECT DISTINCT ?instance WHERE {{?inNode <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@IN_NODE_TYPE@>} {?outNode <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <@OUT_NODE_TYPE@>}{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <@REL_TYPE@>} {?inNode ?rel ?outNode} {?rel <@PROP_TYPE@> ?instance}}";
	Logger logger = Logger.getLogger(getClass());


	@Override
	public void buildQuery() {
		semossQuery.setQueryType(SPARQLConstants.SELECT);
		semossQuery.setDisctinct(true);
		buildQueryForSelectedPath();
		buildQueryForCount();


		semossQuery.createQuery();
	}

	private void buildQueryForSelectedPath()
	{


		ArrayList<ArrayList<String>> tripleArray = (ArrayList<ArrayList<String>>) allJSONHash.get(relArrayKey);
		ArrayList<ArrayList<String>> tripleVarArray = (ArrayList<ArrayList<String>>) allJSONHash.get(relVarArrayKey);
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
			if (!varList.contains(subjectName))
			{
				varList.add(subjectName);
				//store node info
				Hashtable<String, String> elementHash = new Hashtable<String, String>();
				elementHash.put(varKey, subjectName);
				elementHash.put(uriKey, subjectURI);
				String query = this.nodeInstanceQuery.replace("@NODE_TYPE@", subjectURI);
				logger.info("NODE QUERY : " + query);
				elementHash.put(queryKey, query);
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
				String query = this.relInstanceQuery.replace("@IN_NODE_TYPE@", subjectURI).replace("@OUT_NODE_TYPE@", objectURI).replace("@REL_TYPE@", predURI);
				logger.info("REL QUERY : " + query);
				elementHash.put(queryKey, query);
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
				String query = this.nodeInstanceQuery.replace("@NODE_TYPE@", objectURI);
				logger.info("NODE QUERY : " + query);
				elementHash.put(queryKey, query);
				varObjHash.put(objectName, elementHash);
			}
		}

		for (int i=0;i<varList.size();i++)
		{
			SEMOSSQueryHelper.addSingleReturnVarToQuery(varList.get(i), semossQuery);
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
					String query = this.nodePropQuery.replace("@NODE_TYPE@", varURI).replace("@PROP_TYPE@", propURI);
					logger.info("NODE PROP QUERY : " + query);
					elementHash.put(queryKey, query);
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
					String query = this.relPropQuery.replace("@IN_NODE_TYPE@", predInfoHash.get("Subject")).replace("@OUT_NODE_TYPE@", predInfoHash.get("Object")).replace("@REL_TYPE@", predInfoHash.get("Pred")).replace("@PROP_TYPE@", propURI);
					logger.info("REL QUERY : " + query);
					elementHash.put(queryKey, query);
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
