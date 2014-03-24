package prerna.rdf.query.builder;

import java.util.ArrayList;

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
	static final String relVarArrayKey = "relTripleVars";
	static final String propArrayKey = "propRel";
	static final String propVarArrayKey = "propRelVar";
	static final String xAxisKey = "xAxisName";
	static final String yAxisKey = "yAxisName";
	static final String heatNameKey = "heatValueName";
	static final String operatorKey = "operators";
	static final String customHeatString = "customHeatQueryString";

	static final int subIdx = 0;
	static final int predIdx = 1;
	static final int objIdx = 2;
	static final int propIdx = 0;

	
	@Override
	public void buildQuery() {
		String option = (String) allJSONHash.get("option");
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

		
		String xAxis = (String) allJSONHash.get("xAxisName");
		String yAxis = (String) allJSONHash.get("yAxisName");

		SEMOSSQueryHelper.addSingleReturnVarToQuery(xAxis, semossQuery);
		SEMOSSQueryHelper.addSingleReturnVarToQuery(yAxis, semossQuery);
		
		ArrayList<String> groupList  = new ArrayList<String>();
		groupList.add(xAxis);
		groupList.add(yAxis);
		SEMOSSQueryHelper.addGroupByToQuery(groupList, semossQuery);
		
		ArrayList<ArrayList<String>> tripleArray = (ArrayList<ArrayList<String>>) allJSONHash.get("relTriples");
		ArrayList<ArrayList<String>> tripleVarArray = (ArrayList<ArrayList<String>>) allJSONHash.get("relVarTriples");
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
		
	}
	
	private void buildQueryForCount()
	{
		String heatValue = "";
		if((String)allJSONHash.get(heatNameKey)!=null && !((String)allJSONHash.get(heatNameKey)).isEmpty())
		{
			heatValue = (String)allJSONHash.get(heatNameKey);
		}
		else
			heatValue = "HeatValue";
		String yAxis = (String) allJSONHash.get("yAxisName");
		ISPARQLReturnModifier mod;
		mod = SEMOSSQueryHelper.createReturnModifier(yAxis, SPARQLAbstractReturnModifier.COUNT);
		SEMOSSQueryHelper.addSingleReturnVarToQuery(heatValue, mod, semossQuery);
	}
	private void buildQueryForEdgeProp()
	{
		
	}

	private void buildQueryForNodeProp()
	{
		
	}

	private void buildQueryForCustom()
	{
		
	}


}
