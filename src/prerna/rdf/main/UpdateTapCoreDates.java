package prerna.rdf.main;

public class UpdateTapCoreDates {

//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadAll("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
//
//		String tapCoreSmss = "C:\\workspace\\Semoss_Dev\\db\\TAP_Core_Data__133db94b-4371-4763-bff9-edf7e5ed021b.smss";
//		BigDataEngine engine = new BigDataEngine();
//		engine.open(tapCoreSmss);
//		
//		String[] systemProps = new String[] {
//			"ATO_Date",
//			"End_of_Support_Date",
//			"CertificationDate"
//		};
//		
//		for(String p : systemProps) {
//			int counter = 0;
//			String propUri = "http://semoss.org/ontologies/Relation/Contains/" + p;
//			String query = "select distinct ?system ?dateprop where {"
//					+ "{?system <" + RDF.TYPE.toString() + "> <http://semoss.org/ontologies/Concept/System> }"
//					+ "{?system <" + propUri + "> ?dateprop}"
//					+ "}";
//			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
//			while(wrapper.hasNext()) {
//				IHeadersDataRow data = wrapper.next();
//				Object[] row = data.getValues();
//				Object[] raw = data.getRawValues();
//				if( !(row[1] instanceof SemossDate)) {
//					counter++;
//					// remove and re-add
//					engine.removeStatement(new Object[] {raw[0].toString(), propUri, row[1], false});
//					SemossDate date = new SemossDate(row[1].toString(), "yyyy-MM-dd");
//					engine.addStatement(new Object[] {raw[0].toString(), propUri, date.getDate(), false});
//				}
//			}
//			
//			System.out.println(p + " updated " + counter + " times");
//		}
//		
//		System.out.println("Done updating");
//		
//		for(String p : systemProps) {
//			String query = "select distinct ?system ?dateprop where {"
//					+ "{?system <" + RDF.TYPE.toString() + "> <http://semoss.org/ontologies/Concept/System> }"
//					+ "{?system <http://semoss.org/ontologies/Relation/Contains/" + p + "> ?dateprop}"
//					+ "}";
//			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
//			while(wrapper.hasNext()) {
//				IHeadersDataRow data = wrapper.next();
//				Object[] row = data.getValues();
//				if( !(row[1] instanceof SemossDate)) {
//					System.out.println(p + " not stored as date");
//				}
//			}
//		}
//		
//		engine.commit();
//	}
	
}
