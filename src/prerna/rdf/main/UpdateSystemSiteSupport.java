package prerna.rdf.main;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Utility;

public class UpdateSystemSiteSupport {

//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
//
//		Map<String, String> origToReplacement = processFile("C:\\Users\\mahkhalil\\Desktop\\SystemSiteSupportGLItem.csv");
//		
//		List<Object[]> deleteTriples = new Vector<Object[]>();
//		List<Object[]> addTriples = new Vector<Object[]>();
//		
//		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\TAP_Portfolio.smss";
//		IDatabase pEng = new BigDataEngine();
//		pEng.openDB(engineProp);
//		pEng.setEngineId("TAP_Portfolio");
//		DIHelper.getInstance().setLocalProperty("TAP_Portfolio", pEng);
//
//		String query = "select distinct ?systemsitesupportglitem ?system ?dcsite ?fy ?gltag ?cost "
//				+ "where { "
//				+ "{?systemsitesupportglitem a <http://semoss.org/ontologies/Concept/SystemSiteSupportGLItem>} "
//				+ "{?system a <http://semoss.org/ontologies/Concept/System>} "
//				+ "{?dcsite a <http://semoss.org/ontologies/Concept/DCSite>} "
//				+ "{?system <http://semoss.org/ontologies/Relation/Has> ?systemsitesupportglitem} "
//				+ "{?dcsite <http://semoss.org/ontologies/Relation/Has> ?systemsitesupportglitem} "
//				+ "{?systemsitesupportglitem <http://semoss.org/ontologies/Relation/OccursIn> ?fy} "
//				+ "{?systemsitesupportglitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag} "
//				+ "{?systemsitesupportglitem <http://semoss.org/ontologies/Relation/Contains/Cost> ?cost} "
//				+ "}";
//		
//		int count = 0;
//		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(pEng, query);
//		while(it.hasNext()) {
//			IHeadersDataRow row = it.next();
//			Object[] values = row.getValues();
//			String dcsite = values[2].toString();
//			if(origToReplacement.containsKey(dcsite)) {
//				performReplacement(pEng, addTriples, deleteTriples, values, origToReplacement.get(dcsite));
//				count++;
//			}
//		}
//		System.out.println("Found total number = " + count);
//
//		for(Object[] del : deleteTriples) {
//			pEng.doAction(ACTION_TYPE.REMOVE_STATEMENT, del);
//		}
//		
//		for(Object[] add : addTriples) {
//			pEng.doAction(ACTION_TYPE.ADD_STATEMENT, add);
//		}
//		
//		System.out.println("Done");
//		
//		count = 0;
//		it = WrapperManager.getInstance().getRawWrapper(pEng, query);
//		while(it.hasNext()) {
//			IHeadersDataRow row = it.next();
//			Object[] values = row.getValues();
//			String dcsite = values[2].toString();
//			if(origToReplacement.containsKey(dcsite)) {
//				count++;
//			}
//		}
//		System.out.println("Found total number should now be 0.  It is = " + count);
//		
//		pEng.commit();
//	}
	
	private static void performReplacement(IDatabaseEngine eng, List<Object[]> addTriples, List<Object[]> deleteTriples, Object[] values, String newSiteName) {
		String sysSiteGl = values[0].toString();
		String sys = values[1].toString();
		String dcsite = values[2].toString();
		String fy = values[3].toString();
		String gltag = values[4].toString();
		double cost = (double) values[5];

		// delete the entire sys site gl item
		Set<String> binding = new HashSet<String>();
		binding.add("http://health.mil/ontologies/Concept/SystemSiteSupportGLItem/" + sysSiteGl);
		deleteAllRDFConnectionsToConcept(eng, binding, deleteTriples);
		
		// first get all the stuff to delete
		String semossRel = "http://semoss.org/ontologies/Relation";
		String semossHasPred = "http://semoss.org/ontologies/Relation/Has";
		String healthHasPred = "http://health.mil/ontologies/Relation/Has";
		String semossOccursPred = "http://semoss.org/ontologies/Relation/OccursIn";
		String healthOccursPred = "http://health.mil/ontologies/Relation/OccursIn";
		String semossTaggedPred = "http://semoss.org/ontologies/Relation/TaggedBy";
		String healthTaggedPred = "http://health.mil/ontologies/Relation/TaggedBy";

		// now we reconnect to new
		String newNodeName = sys + "%" + newSiteName + "%SiteSupport%" + fy;
		String newSysSiteGl = "http://health.mil/ontologies/Concept/SystemSiteSupportGLItem/" + newNodeName;
		// add new gl
		addTriples.add(new Object[]{newSysSiteGl, RDF.TYPE, "http://semoss.org/ontologies/Concept/SystemSiteSupportGLItem", true});
		addTriples.add(new Object[]{newSysSiteGl, RDFS.LABEL, newNodeName, false});
		// add new dcsite
		String newDcsite = "http://health.mil/ontologies/Concept/DCSite/" + newSiteName;
		addTriples.add(new Object[]{newDcsite, RDF.TYPE, "http://semoss.org/ontologies/Concept/DCSite", true});
		addTriples.add(new Object[]{newDcsite, RDFS.LABEL, newNodeName, false});
		
		// system to gl
		{
			String predSysGlName = sys + ":" + newNodeName;
			String predSysGl = healthHasPred + "/" + predSysGlName;
			addTriples.add(new Object[]{
					"http://health.mil/ontologies/Concept/System/" + sys, 
					semossRel, 
					newSysSiteGl, 
					true});
			addTriples.add(new Object[]{
					"http://health.mil/ontologies/Concept/System/" + sys, 
					semossHasPred, 
					newSysSiteGl, 
					true});
			addTriples.add(new Object[]{
					"http://health.mil/ontologies/Concept/System/" + sys, 
					predSysGl, 
					newSysSiteGl, 
					true});
			addTriples.add(new Object[]{
					predSysGl, 
					RDFS.SUBPROPERTYOF.toString(), 
					semossRel, 
					true});
			addTriples.add(new Object[]{
					predSysGl,
					RDFS.SUBPROPERTYOF.toString(), 
					semossHasPred, 
					true});
		}
		// dcsite to gl
		{
			String predSiteGlName = newSiteName + ":" + newNodeName;
			String predSiteGl = healthHasPred + "/" + predSiteGlName;

			addTriples.add(new Object[]{
					"http://health.mil/ontologies/Concept/DCSite/" + newSiteName, 
					semossRel, 
					newSysSiteGl, 
					true});
			addTriples.add(new Object[]{
					"http://health.mil/ontologies/Concept/DCSite/" + newSiteName, 
					semossHasPred, 
					newSysSiteGl, 
					true});
			addTriples.add(new Object[]{
					"http://health.mil/ontologies/Concept/DCSite/" + newSiteName, 
					predSiteGl, 
					newSysSiteGl, 
					true});
			addTriples.add(new Object[]{
					predSiteGl,
					RDFS.LABEL.toString(), 
					predSiteGlName, 
					false});
			addTriples.add(new Object[]{
					predSiteGl, 
					RDFS.SUBPROPERTYOF.toString(), 
					semossRel, 
					true});
			addTriples.add(new Object[]{
					predSiteGl,
					RDFS.SUBPROPERTYOF.toString(), 
					semossHasPred, 
					true});
		}
		// gl to gytag
		{
			String predGlGyTagName = newNodeName + ":" + gltag;
			String predGlGyTag = healthTaggedPred + "/" + predGlGyTagName;

			addTriples.add(new Object[]{
					newSysSiteGl, 
					semossRel, 
					"http://health.mil/ontologies/Concept/GLTag/" + gltag, 
					true});
			addTriples.add(new Object[]{
					newSysSiteGl, 
					semossTaggedPred, 
					"http://health.mil/ontologies/Concept/GLTag/" + gltag, 
					true});
			addTriples.add(new Object[]{
					newSysSiteGl, 
					predGlGyTag, 
					"http://health.mil/ontologies/Concept/GLTag/" + gltag, 
					true});
			addTriples.add(new Object[]{
					predGlGyTag,
					RDFS.LABEL, 
					predGlGyTagName, 
					false});
			addTriples.add(new Object[]{
					predGlGyTag, 
					RDFS.SUBPROPERTYOF.toString(), 
					semossRel, 
					true});
			addTriples.add(new Object[]{
					predGlGyTag,
					RDFS.SUBPROPERTYOF.toString(), 
					semossTaggedPred, 
					true});
		}
		
		// gl to fy
		{
			String predGlFyName = newNodeName + ":" + fy;
			String predGlFy = healthOccursPred + "/" + predGlFyName;

			addTriples.add(new Object[]{
					newSysSiteGl, 
					semossRel, 
					"http://health.mil/ontologies/Concept/FYTag/" + fy, 
					true});
			addTriples.add(new Object[]{
					newSysSiteGl, 
					semossOccursPred, 
					"http://health.mil/ontologies/Concept/FYTag/" + fy, 
					true});
			addTriples.add(new Object[]{
					newSysSiteGl, 
					predGlFy, 
					"http://health.mil/ontologies/Concept/FYTag/" + fy, 
					true});
			addTriples.add(new Object[]{
					predGlFy,
					RDFS.LABEL.toString(), 
					predGlFyName, 
					false});
			addTriples.add(new Object[]{
					predGlFy, 
					RDFS.SUBPROPERTYOF.toString(), 
					semossRel, 
					true});
			addTriples.add(new Object[]{
					predGlFy,
					RDFS.SUBPROPERTYOF.toString(), 
					semossOccursPred, 
					true});
		}
		
		// gl has cost
		{
			String costProp = "http://semoss.org/ontologies/Relation/Contains/Cost";

			addTriples.add(new Object[]{
					newSysSiteGl, 
					costProp, 
					cost, 
					false});
		}
	}

	private static Map<String, String> processFile(String fileLoc) {
		Map<String, String> retMap = new HashMap<String, String>();
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(',');
		helper.parse(fileLoc);
		helper.getHeaders();
		Object[] row = null;
		while((row = helper.getNextRow()) != null) {
			String original = row[0].toString();
			String replacement = row[1].toString();
			
			retMap.put(original, replacement);
		}
		return retMap;
	}

	private static void deleteAllRDFConnectionsToConcept(IDatabaseEngine eng, Set<String> uriBindingList, List<Object[]> deleteTriples) {
		String[] queries = new String[]{
				generateDeleteAllRDFConnectionsToConceptQuery(uriBindingList, true),
				generateDeleteAllRDFConnectionsToConceptQuery(uriBindingList, false)};

		String baseRel = "http://semoss.org/ontologies/Relation";
		for(String query : queries) {
			if(query == null) {
				continue;
			}
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(eng, query);
			String[] names = wrapper.getVariables();
			while(wrapper.hasNext()) {
				ISelectStatement ss = wrapper.next();
				String subURI = ss.getRawVar(names[0]) + "";
				String predURI = ss.getRawVar(names[1]) + "";
				String objURI = ss.getRawVar(names[2]) + "";
				Object label = ss.getVar(names[3]);
				Object propURI = ss.getRawVar(names[4]);
				Object propVal = ss.getVar(names[5]);

				deleteTriples.add(new Object[]{subURI, predURI, objURI, true});
				if(label != null && !label.toString().isEmpty()) {
					deleteTriples.add(new Object[]{predURI, RDFS.LABEL, label, false});
				}
				if(propURI != null && !propURI.toString().isEmpty()) {
					deleteTriples.add(new Object[]{predURI, propURI, propVal, false});
				}

				// need to do a lot of stuff for relationships
				// ignore the base Relation
				if(!predURI.equals(baseRel)) {
					// remove the prefix so we only get RelType/Instance
					// assuming the instance is there..
					String suffix = predURI.replaceAll(".*ontologies/Relation/", "");
					if(suffix.contains("/")) {
						String[] split = suffix.split("/");
						String relType = split[0];

						// delete the sub properties around the instance
						String baseRelationURI = baseRel + "/" + relType;
						deleteTriples.add(new Object[]{predURI, RDFS.SUBPROPERTYOF, baseRel, true});
						deleteTriples.add(new Object[]{predURI, RDFS.SUBPROPERTYOF, baseRelationURI, true});
						deleteTriples.add(new Object[]{predURI, RDF.TYPE, RDF.PROPERTY, true});

						// and delete the base rel directly between the subject and object
						deleteTriples.add(new Object[]{subURI, baseRelationURI, objURI, true});
					}
				}
			}
		}
		// lastly, remove the node and all its props
		removeRDFNodeAndAllProps(eng, uriBindingList, deleteTriples);
	}

	/**
	 * 
	 * @param conceptURI
	 * @param downstream
	 * @return
	 */
	private static String generateDeleteAllRDFConnectionsToConceptQuery(Set<String> conceptURI, boolean downstream) {
		if(conceptURI.isEmpty()) {
			return null;
		}
		StringBuilder query = new StringBuilder("SELECT DISTINCT ?SUB ?PRED ?OBJ ?LABEL ?PROP ?VAL WHERE { ");
		query.append("{ ");
		query.append("{?PRED <").append(RDFS.SUBPROPERTYOF).append("> <http://semoss.org/ontologies/Relation>} ");
		query.append("{?SUB ?PRED ?OBJ} ");
		query.append("OPTIONAL{ ?PRED <").append(RDFS.LABEL).append("> ?LABEL} ");
		query.append("} UNION { ");
		query.append("{?PRED <").append(RDFS.SUBPROPERTYOF).append("> <http://semoss.org/ontologies/Relation>} ");
		query.append("{?SUB ?PRED ?OBJ} ");
		query.append("OPTIONAL{ ?PRED <").append(RDFS.LABEL).append("> ?LABEL} ");
		query.append("{?PROP <").append(RDF.TYPE).append("> <http://semoss.org/ontologies/Relation/Contains>} ");
		query.append("{?PRED ?PROP ?VAL} ");
		query.append("} }");
		if(downstream) {
			query.append("BINDINGS ?SUB {");
		} else {
			query.append("BINDINGS ?OBJ {");
		}
		for(String concept : conceptURI) {
			query.append("(<");
			query.append(concept);
			query.append(">)");
		}
		query.append("}");

		return query.toString();
	}

	private static void removeRDFNodeAndAllProps(IDatabaseEngine eng, Set<String> uriBindingList, List<Object[]> deleteTriples) {
		if(uriBindingList.isEmpty()) {
			return;
		}

		// delete the properties for the instances
		StringBuilder query = new StringBuilder("SELECT DISTINCT ?NODE ?PROP ?VAL WHERE { ");
		query.append("{?PROP <").append(RDF.TYPE).append("> <http://semoss.org/ontologies/Relation/Contains>} ");
		query.append("{?NODE ?PROP ?VAL} } ");
		query.append("BINDINGS ?NODE {");
		for(String concept : uriBindingList) {
			query.append("(<");
			query.append(concept);
			query.append(">)");
		}
		query.append("}");

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(eng, query.toString());
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String nodeURI = ss.getRawVar(names[0]) + "";
			String propURI = ss.getRawVar(names[1]) + "";
			Object propVal = ss.getVar(names[2]);

			deleteTriples.add(new Object[]{nodeURI, propURI, propVal, false});
		}

		// deletes the instances
		String semossBaseConcept = "http://semoss.org/ontologies/Concept";
		for(String nodeURI : uriBindingList) {
			String typeURI = semossBaseConcept + "/" + Utility.getClassName(nodeURI);
			deleteTriples.add(new Object[]{nodeURI, RDF.TYPE, typeURI, true});
		}
	}


}
