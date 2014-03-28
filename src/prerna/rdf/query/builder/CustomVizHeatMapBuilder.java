package prerna.rdf.query.builder;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;

import com.google.gson.internal.StringMap;

import prerna.rdf.query.util.SEMOSSQueryHelper;
import prerna.rdf.query.util.SPARQLAbstractReturnModifier;
import prerna.rdf.query.util.SPARQLConstants;
import prerna.rdf.query.util.ISPARQLReturnModifier;
import prerna.rdf.query.util.TriplePart;

public class CustomVizHeatMapBuilder extends AbstractCustomVizBuilder{
	static final String optionKey = "option";
	static final String MODE_COUNT = "Count";
	static final String MODE_EDGEPROP = "Edge Properties";
	static final String MODE_NODEPROP = "Node Properties";
	static final String MODE_CUSTOM = "Custom";
	static final String relArrayKey = "relTriples";
	static final String relVarArrayKey = "relVarTriples";
	static final String propArrayKey = "propRel";
	static final String propVarArrayKey = "propRelVar";
	static final String xAxisKey = "xAxisName";
	static final String yAxisKey = "yAxisName";
	static final String heatNameKey = "heatValueName";
	static final String operatorKey = "operators";
	static final String customHeatString = "customHeatQueryString";
	static final String uriKey = "uri";
	static final String tripleIdxKey = "tripleIndex";
	static final String edgePropKey = "edgeProps";
	static final String nodePropKey = "nodeProps";
	static final String nodePropName = "nodeName";
	
	static final int subIdx = 0;
	static final int predIdx = 1;
	static final int objIdx = 2;
	static final int propIdx = 0;

	
	@Override
	public void buildQuery() {
		String option = (String) allJSONHash.get(optionKey);
		semossQuery.setQueryType(SPARQLConstants.SELECT);
		semossQuery.setDisctinct(true);
		buildQueryCommonParts();
		if(option.equals(MODE_COUNT))
			buildQueryForCount();
		else if (option.equals(MODE_EDGEPROP))
			buildQueryForEdgeProp();
		else if (option.equals(MODE_NODEPROP))
			buildQueryForNodeProp();
		else if (option.equals(MODE_CUSTOM))
			buildQueryForCustom();
			
		semossQuery.createQuery();
	}
	
	private void buildQueryCommonParts()
	{

		
		String xAxis = (String) allJSONHash.get(xAxisKey);
		String yAxis = (String) allJSONHash.get(yAxisKey);

		SEMOSSQueryHelper.addSingleReturnVarToQuery(xAxis, semossQuery);
		SEMOSSQueryHelper.addSingleReturnVarToQuery(yAxis, semossQuery);
		
		ArrayList<String> groupList  = new ArrayList<String>();
		groupList.add(xAxis);
		groupList.add(yAxis);
		SEMOSSQueryHelper.addGroupByToQuery(groupList, semossQuery);
		

		//because of inferencing, we cannot use this..
//		for (int tripleIdx = 0; tripleIdx<tripleArray.size(); tripleIdx++)
//		{
//			String subjectName = tripleVarArray.get(tripleIdx).get(subIdx);
//			String predURIName = tripleVarArray.get(tripleIdx).get(predIdx);
//			String objURIName =	tripleVarArray.get(tripleIdx).get(objIdx);
//			String subjectURI = tripleArray.get(tripleIdx).get(subIdx);
//			String predURI = tripleArray.get(tripleIdx).get(predIdx);
//			String objURI = tripleArray.get(tripleIdx).get(objIdx);
//			SEMOSSQueryHelper.addConceptTypeTripleToQuery(subjectName, subjectURI, semossQuery);
//			SEMOSSQueryHelper.addConceptTypeTripleToQuery(objURIName, objURI, semossQuery);
//			SEMOSSQueryHelper.addRelationTypeTripleToQuery(predURIName, predURI, semossQuery);
//			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(subjectName, predURIName, objURIName, semossQuery);
//		}
		
		//lets' use this version for now

		
	}
	
	private void buildQueryForCount()
	{
		ArrayList<ArrayList<String>> tripleArray = (ArrayList<ArrayList<String>>) allJSONHash.get(relArrayKey);
		ArrayList<ArrayList<String>> tripleVarArray = (ArrayList<ArrayList<String>>) allJSONHash.get(relVarArrayKey);
		for (int tripleIdx = 0; tripleIdx<tripleArray.size(); tripleIdx++)
		{
			String subjectName = tripleVarArray.get(tripleIdx).get(subIdx);
			String objURIName =	tripleVarArray.get(tripleIdx).get(objIdx);
			String subjectURI = tripleArray.get(tripleIdx).get(subIdx);
			String predURI = tripleArray.get(tripleIdx).get(predIdx);
			String objURI = tripleArray.get(tripleIdx).get(objIdx);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(subjectName, subjectURI, semossQuery);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(objURIName, objURI, semossQuery);
			//add relationship triples with the relationURI
			SEMOSSQueryHelper.addGenericTriple(subjectName, TriplePart.VARIABLE, predURI, TriplePart.URI, objURIName, TriplePart.VARIABLE, semossQuery);
		}
		String heatValue = "";
		if((String)allJSONHash.get(heatNameKey)!=null && !((String)allJSONHash.get(heatNameKey)).isEmpty())
		{
			heatValue = (String)allJSONHash.get(heatNameKey);
		}
		else
			heatValue = "HeatValue";
		String yAxis = (String) allJSONHash.get(yAxisKey);
		ISPARQLReturnModifier mod;
		mod = SEMOSSQueryHelper.createReturnModifier(yAxis, SPARQLAbstractReturnModifier.COUNT);
		SEMOSSQueryHelper.addSingleReturnVarToQuery(heatValue, mod, semossQuery);
	}
	
	private void buildQueryForEdgeProp()
	{
		ArrayList<ArrayList<String>> tripleArray = (ArrayList<ArrayList<String>>) allJSONHash.get(relArrayKey);
		ArrayList<ArrayList<String>> tripleVarArray = (ArrayList<ArrayList<String>>) allJSONHash.get(relVarArrayKey);
		StringMap edgePropMap =(StringMap)allJSONHash.get(edgePropKey);
		ArrayList<Object> propList = new ArrayList<Object>();
		
		for (int tripleIdx = 0; tripleIdx<tripleArray.size(); tripleIdx++)
		{
			String subjectName = tripleVarArray.get(tripleIdx).get(subIdx);
			String predURIName = tripleVarArray.get(tripleIdx).get(predIdx);
			String objURIName =	tripleVarArray.get(tripleIdx).get(objIdx);
			String subjectURI = tripleArray.get(tripleIdx).get(subIdx);
			String predURI = tripleArray.get(tripleIdx).get(predIdx);
			String objURI = tripleArray.get(tripleIdx).get(objIdx);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(subjectName, subjectURI, semossQuery);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(objURIName, objURI, semossQuery);
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(predURIName, predURI, semossQuery);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(subjectName, predURIName, objURIName, semossQuery);
		}
		Iterator it = edgePropMap.entrySet().iterator();
	    while(it.hasNext()){
	    	Entry pairs = (Entry)it.next();
			StringMap propHash = (StringMap) pairs.getValue();
			int propIdx = (int)((double) propHash.get(tripleIdxKey));
			String propURI = (String) propHash.get(uriKey);
			String predVar = tripleVarArray.get(propIdx).get(predIdx);
			SEMOSSQueryHelper.addGenericTriple(predVar, TriplePart.VARIABLE, propURI, TriplePart.URI, pairs.getKey()+"", TriplePart.VARIABLE, semossQuery);
			propList.add(propIdx, pairs.getKey());
	    }
		ISPARQLReturnModifier mod;
		ArrayList<String> opList = (ArrayList<String>) allJSONHash.get(operatorKey);
		//first multiply the weights
		mod = SEMOSSQueryHelper.createReturnModifier(propList, opList);
		mod = SEMOSSQueryHelper.createReturnModifier(mod, SPARQLAbstractReturnModifier.SUM);
		
		
		String heatValue = "";
		if((String)allJSONHash.get(heatNameKey)!=null && !((String)allJSONHash.get(heatNameKey)).isEmpty())
		{
			heatValue = (String)allJSONHash.get(heatNameKey);
		}
		else
			heatValue = "HeatValue";
		SEMOSSQueryHelper.addSingleReturnVarToQuery(heatValue, mod, semossQuery);
	}

	private void buildQueryForNodeProp()
	{
		ArrayList<ArrayList<String>> tripleArray = (ArrayList<ArrayList<String>>) allJSONHash.get(relArrayKey);
		ArrayList<ArrayList<String>> tripleVarArray = (ArrayList<ArrayList<String>>) allJSONHash.get(relVarArrayKey);
		StringMap nodePropMap =(StringMap)allJSONHash.get(nodePropKey);
		ArrayList<Object> propList = new ArrayList<Object>();
		
		for (int tripleIdx = 0; tripleIdx<tripleArray.size(); tripleIdx++)
		{
			String subjectName = tripleVarArray.get(tripleIdx).get(subIdx);
			String predURIName = tripleVarArray.get(tripleIdx).get(predIdx);
			String objURIName =	tripleVarArray.get(tripleIdx).get(objIdx);
			String subjectURI = tripleArray.get(tripleIdx).get(subIdx);
			String predURI = tripleArray.get(tripleIdx).get(predIdx);
			String objURI = tripleArray.get(tripleIdx).get(objIdx);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(subjectName, subjectURI, semossQuery);
			SEMOSSQueryHelper.addConceptTypeTripleToQuery(objURIName, objURI, semossQuery);
			SEMOSSQueryHelper.addRelationTypeTripleToQuery(predURIName, predURI, semossQuery);
			SEMOSSQueryHelper.addRelationshipVarTripleToQuery(subjectName, predURIName, objURIName, semossQuery);
		}
		Iterator it = nodePropMap.entrySet().iterator();
	    while(it.hasNext()){
	    	Entry pairs = (Entry)it.next();
			StringMap propHash = (StringMap) pairs.getValue();
			int propIdx = (int)((double) propHash.get(tripleIdxKey));
			String propURI = (String) propHash.get(uriKey);
			String nodeVar = (String) propHash.get(nodePropName);
			SEMOSSQueryHelper.addGenericTriple(nodeVar, TriplePart.VARIABLE, propURI, TriplePart.URI, pairs.getKey()+"", TriplePart.VARIABLE, semossQuery);
			propList.add(propIdx, pairs.getKey());
	    }
		ISPARQLReturnModifier mod;
		ArrayList<String> opList = (ArrayList<String>) allJSONHash.get(operatorKey);
		//first multiply the weights
		mod = SEMOSSQueryHelper.createReturnModifier(propList, opList);
		mod = SEMOSSQueryHelper.createReturnModifier(mod, SPARQLAbstractReturnModifier.SUM);
		
		
		String heatValue = "";
		if((String)allJSONHash.get(heatNameKey)!=null && !((String)allJSONHash.get(heatNameKey)).isEmpty())
		{
			heatValue = (String)allJSONHash.get(heatNameKey);
		}
		else
			heatValue = "HeatValue";
		SEMOSSQueryHelper.addSingleReturnVarToQuery(heatValue, mod, semossQuery);
	}

	private void buildQueryForCustom()
	{
		
	}
}
