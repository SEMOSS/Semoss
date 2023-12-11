//package prerna.rdf.main;
//
//import prerna.engine.api.ISelectStatement;
//import prerna.engine.api.ISelectWrapper;
//import prerna.engine.impl.rdf.BigDataEngine;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.test.TestUtilityMethods;
//import prerna.util.DIHelper;
//
//class TestGson {
//	
//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadDIHelper();
//
//		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Forms_TAP_Core_Data.smss";
//		BigDataEngine test5 = new BigDataEngine();
//		test5.open(engineProp);
//		test5.setEngineId("Forms_TAP_Core_Data");
//		DIHelper.getInstance().setLocalProperty("Forms_TAP_Core_Data", test5);
//
////		Vector<String> concepts = test5.getConcepts2(false);
////		concepts.remove("http://semoss.org/ontologies/Concept");
////		System.out.println("HAVE CONCEPTS ::: " + concepts);
////		for(String concept : concepts) {
////			List<String> properties = test5.getProperties4Concept2(concept, false);
////			if(properties != null && properties.size() > 0) {
////				System.out.println("CONCEPT == " + concept + " ::: PROPERTIES = " + properties);
////			}
////		}
//		
//		String query = "SELECT DISTINCT ?SYSTEM WHERE {?SYSTEM a <http://semoss.org/ontologies/Concept/System>}";
//		
////		String query = "SELECT DISTINCT ?S ?P ?0 WHERE {?S ?P ?0}";
////		
////		query = "SELECT DISTINCT ?S ?predicate ?O WHERE { " 
////				+ "BIND(<http://semoss.org/ontologies/Concept/DayOfWeek_1> AS ?S) "
//////				+ "BIND(<" + OWL.DatatypeProperty + "> AS ?P) "
////
////				+ "{?S <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> } "
////				+ "{?O <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> } "
////				+ "{?S <http://www.w3.org/2002/07/owl#DatatypeProperty> ?O } "
////
//////				+ "{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} "
//////				+ "{?concept <http://www.w3.org/2002/07/owl#DatatypeProperty> ?property} "
////				+ "}";
//		
//		ISelectWrapper manager = WrapperManager.getInstance().getSWrapper(test5, query);
//		int count = 0;
//		while(manager.hasNext()) {
//			ISelectStatement row = manager.next();
//			System.out.println(row.getRawVar("SYSTEM"));
//			count++;
////			System.out.println("S = " + row.getRawVar("S") + " ::: " + "P = " + row.getRawVar("P") + " ::: " + "O = " + row.getRawVar("O"));
//
////			System.out.println("concept = " + row.getRawVar("concept") + " ::: " + "property = " + row.getRawVar("property"));
//		}
//		
//		System.out.println("COUNT ::: " + count);
//		System.out.println("DONE");
//	}
//	
//}
