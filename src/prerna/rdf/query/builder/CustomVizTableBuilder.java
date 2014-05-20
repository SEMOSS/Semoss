package prerna.rdf.query.builder;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.query.util.SEMOSSQueryHelper;
import prerna.rdf.query.util.SPARQLConstants;
import prerna.rdf.query.util.TriplePart;
import prerna.util.Utility;

public class CustomVizTableBuilder extends AbstractCustomVizBuilder{

	static final String relArrayKey = "relTriples";
	static final String relVarArrayKey = "relVarTriples";
	ArrayList<String> varList = new ArrayList<String>();
	Hashtable<String, String> uriList = new Hashtable<String, String>();
	Hashtable<String, Hashtable<String,String>> predHash = new Hashtable<String, Hashtable<String,String>>();
	IEngine coreEngine = null;
	static final int subIdx = 0;
	static final int predIdx = 1;
	static final int objIdx = 2;
	static final int propIdx = 0;


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
				uriList.put(subjectName, subjectURI);
			}
			if (!varList.contains(predName))
			{
				Hashtable<String, String> predInfoHAsh = new Hashtable<String,String>();
				predInfoHAsh.put("Subject", subjectURI);
				predInfoHAsh.put("Pred", predURI);
				predInfoHAsh.put("Object", objectURI);
				varList.add(predName);
				uriList.put(predName, predURI);
				predHash.put(predName,  predInfoHAsh);
			}
			if (!varList.contains(objectName))
			{
				varList.add(objectName);
				uriList.put(objectName, objectURI);
			}
		}

		for (int i=0;i<varList.size();i++)
		{
			SEMOSSQueryHelper.addSingleReturnVarToQuery(varList.get(i), semossQuery);
		}

	}

	private void buildQueryForCount()
	{
		for (int i=0;i<varList.size();i++)
		{
			String varName = varList.get(i);
			String varURI = uriList.get(varName);
			if (!predHash.containsKey(varName))
			{
				String nodePropQuery = "SELECT DISTINCT ?entity WHERE {{?source <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+varURI+">} {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?source ?entity ?prop }}";
				Vector<String> propV = coreEngine.getEntityOfType(nodePropQuery);
				for (int propIdx=0 ; propIdx<propV.size(); propIdx++)
				{
					String propURI = propV.get(propIdx);
					String propName = varName + "__" + Utility.getInstanceName(propURI);
					SEMOSSQueryHelper.addGenericTriple(varName, TriplePart.VARIABLE, propURI, TriplePart.URI, propName, TriplePart.VARIABLE, semossQuery);
					SEMOSSQueryHelper.addSingleReturnVarToQuery(propName, semossQuery);
				}
			}
			else
			{
				Hashtable<String, String> predInfoHash = predHash.get(varName);

				String edgePropQuery = "SELECT DISTINCT ?entity WHERE {{?source <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + predInfoHash.get("Subject") + ">} {?target <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + predInfoHash.get("Object") + "> } {?verb <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <" + predInfoHash.get("Pred") + "> }{?source ?verb ?target;} {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?verb ?entity ?prop }}";
				Vector<String> propV = coreEngine.getEntityOfType(edgePropQuery);
				for (int propIdx=0 ; propIdx<propV.size(); propIdx++)
				{
					String propURI = propV.get(propIdx);
					String propName = varName + "__" + Utility.getInstanceName(propURI);
					SEMOSSQueryHelper.addGenericTriple(varName, TriplePart.VARIABLE, propURI, TriplePart.URI, propName, TriplePart.VARIABLE, semossQuery);
					SEMOSSQueryHelper.addSingleReturnVarToQuery(propName, semossQuery);

				}
			}
		}




	}

	public void setEngine(IEngine coreEngine)
	{
		this.coreEngine = coreEngine;
	}
}
