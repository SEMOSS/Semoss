package prerna.rdf.main;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;

public class TestLocalDB {

	public static void main(String[] args) throws Exception {
		
		TestUtilityMethods.loadDIHelper();

//		//TODO: put in correct path for your database
//		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
//		IEngine coreEngine = new BigDataEngine();
//		coreEngine.setEngineName(Constants.LOCAL_MASTER_DB_NAME);
//		coreEngine.openDB(engineProp);
//		//TODO: put in correct db name
//		coreEngine.setEngineName(Constants.LOCAL_MASTER_DB_NAME);
//		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);
//		
//		String query = "SELECT DISTINCT ?s ?p ?o where { {?s ?p ?o} }";
//		
////		File f = new File("C:\\workspace\\Semoss_Dev\\LocalMasterTriples.txt");
////		if(f.exists()) {
////			f.delete();
////		}
////		f.createNewFile();
////		FileWriter writer = new FileWriter(f);
////		
////		System.out.println("Start query...");
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(coreEngine, query);
////		while(wrapper.hasNext()) {
////			IHeadersDataRow row = wrapper.next();
////			writer.write(row.toRawString());
////		}
////		System.out.println("End query...");
////
////		writer.close();
//
//		String conceptURI = "http://semoss.org/ontologies/Concept/System";
//		String upstreamQuery = "SELECT DISTINCT ?someEngine ?fromConcept ?fromLogical WHERE {"
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
//				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
//				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/logical> ?fromLogical}"
//				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/logical> <" + conceptURI + ">}" // change this back to logical
//				+ "{?toConceptComposite ?someRel ?conceptComposite}"
//				//+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
//				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//				+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
//				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
//				+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
//				+ "&& ?toConcept != ?someEngine"
//				+ "&& ?fromLogical != <" + conceptURI + ">"
//				+")}";
//		
//		System.out.println("Start query...");
//		wrapper = WrapperManager.getInstance().getRawWrapper(coreEngine, upstreamQuery);
//		while(wrapper.hasNext()) {
//			IHeadersDataRow row = wrapper.next();
//			System.out.println(row.toRawString());
//		}
//		System.out.println("End query...");
//
//		String downstreamQuery = "SELECT DISTINCT ?someEngine ?fromConcept ?fromLogical WHERE {"
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
//				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/presentin> ?someEngine}"
//				+ "{?conceptComposite <" + RDF.TYPE + "> ?fromConcept}"
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/logical> ?fromLogical}"
//				+ "{?toConceptComposite <http://semoss.org/ontologies/Relation/logical> <" + conceptURI + ">}" // change this back to logical
//				+ "{?conceptComposite ?someRel ?toConceptComposite}"
//				//+ "{?someRel <" + RDFS.subPropertyOf + "> <http://semoss.org/ontologies/Relation>}"
//				+ "{?conceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//				+ "{?toConceptComposite <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//				+ "{?fromConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//				+ "{?toConcept <" + RDFS.subClassOf + "> <http://semoss.org/ontologies/Concept>}"
//				+ "FILTER(?fromConcept != <http://semoss.org/ontologies/Concept> "
//				+ "&& ?toConcept != <http://semoss.org/ontologies/Concept>"
//				+ "&& ?someRel != <http://semoss.org/ontologies/Relation>"
//				+ "&& ?toConcept != ?someEngine"
//				+ "&& ?fromLogical != <" + conceptURI + ">"
//				+")}";
//		
//		System.out.println("Start query...");
//		wrapper = WrapperManager.getInstance().getRawWrapper(coreEngine, downstreamQuery);
//		while(wrapper.hasNext()) {
//			IHeadersDataRow row = wrapper.next();
//			System.out.println(row.toRawString());
//		}
//		System.out.println("End query...");
//		
		
		//TODO: put in correct path for your database
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Forms_TAP_Core_Data.smss";
		BigDataEngine coreEngine = new BigDataEngine();
		coreEngine.setEngineName("FormsEngine");
		coreEngine.openDB(engineProp);
		//TODO: put in correct db name
		coreEngine.setEngineName("FormsEngine");
		DIHelper.getInstance().setLocalProperty("FormsEngine", coreEngine);
		
		String systemUri = "http://health.mil/ontologies/Concept/System/CIS-Essentris";
		String propertyUri = "http://semoss.org/ontologies/Relation/Contains/User_Consoles";
		Object propToRemove = "NA";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "NaN";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "\"NA\"";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		
//		
//		propertyUri = "http://semoss.org/ontologies/Relation/Contains/Number_of_Users";
//		propToRemove = "NA";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "NaN";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "\"NA\"";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = 1.0;
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = 90.0;
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = 198.0;
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
		
		propertyUri = "http://semoss.org/ontologies/Relation/Contains/Availability-Required";
//		propToRemove = 99.9;
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "99.9";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "\"99.0\"^^<http://www.w3.org/2001/XMLSchema#double>";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
		
		
//		propToRemove = "NA";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "NaN";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		coreEngine.addStatement(new Object[]{systemUri, propertyUri, 1.0, false});
		
//		propertyUri = "http://semoss.org/ontologies/Relation/Contains/Availability-Actual";
//		propToRemove = "NA";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "NaN";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		


//		propertyUri = "http://semoss.org/ontologies/Relation/Contains/AvailabilityRequired";
//		propToRemove = 99.9;
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "NA";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "NaN";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		coreEngine.addStatement(new Object[]{systemUri, propertyUri, 1.0, false});
//		
//		propertyUri = "http://semoss.org/ontologies/Relation/Contains/AvailabilityActual";
//		propToRemove = "NA";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "NaN";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
		
		
		
//		propertyUri = "http://semoss.org/ontologies/Relation/Contains/Transaction_Count";
//		propToRemove = "NA";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "NaN";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = "\"NA\"";
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = 99.9;
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
//		propToRemove = 131000.0;
//		coreEngine.removeStatement(new Object[]{systemUri, propertyUri, propToRemove, false});
		
//		coreEngine.commit();
//		
//		String deleteQ = "DELETE WHERE { <http://health.mil/ontologies/Concept/System/CIS-Essentris> <http://semoss.org/ontologies/Relation/Contains/Availability-Required> ?Val . }";
//		coreEngine.removeData(deleteQ);
//		coreEngine.commit();
//		
//		coreEngine.addStatement(new Object[]{systemUri, propertyUri, 1.0, false});
//		coreEngine.commit();
		
		String query = "SELECT DISTINCT ?System ?Prop ?Val WHERE {BIND(<http://health.mil/ontologies/Concept/System/CIS-Essentris> AS ?System) {?Prop a <http://semoss.org/ontologies/Relation/Contains>} {?System ?Prop ?Val}}";
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(coreEngine, query);
		while(wrapper.hasNext()) {
			IHeadersDataRow row = wrapper.next();
			System.out.println(row.toRawString());
		}
	}
}
