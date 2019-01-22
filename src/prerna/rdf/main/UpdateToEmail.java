package prerna.rdf.main;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.poi.main.helper.excel.ExcelBlock;
import prerna.poi.main.helper.excel.ExcelRange;
import prerna.poi.main.helper.excel.ExcelSheetFileIterator;
import prerna.poi.main.helper.excel.ExcelSheetPreProcessor;
import prerna.poi.main.helper.excel.ExcelWorkbookFileHelper;
import prerna.poi.main.helper.excel.ExcelWorkbookFilePreProcessor;
import prerna.query.querystruct.ExcelQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;

public class UpdateToEmail {

	public static void main(String[] args) throws Exception {
		//////////////////////////////////////////////////////////////////////
		
		/*
		 * User required paths
		 */
		
		TestUtilityMethods.loadAll("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");

		String accessMappingPath = "C:\\Users\\SEMOSS\\Desktop\\MHS SPECIFIC CHANGES\\Access Mapping.xlsx";
		String sheetName = "Mapping Tab";
		String form_builder_engine_smss = "C:\\workspace\\Semoss_Dev\\db\\form_builder_engine.smss";
		String forms_tap_core_id = "132db94b-4371-4763-bff9-edf7e5ed021b";
		String forms_tap_core_smss = "C:\\workspace\\Semoss_Dev\\db\\Forms_TAP_Core_Data__132db94b-4371-4763-bff9-edf7e5ed021b.smss";

		
		///////////////////////////////////////////////////////////////////////
		
		Map<String, String> mappingToEmail = new HashMap<String, String>();

		ExcelWorkbookFileHelper helper = new ExcelWorkbookFileHelper();
		helper.parse(accessMappingPath);

		ExcelWorkbookFilePreProcessor wProcessor = new ExcelWorkbookFilePreProcessor();
		wProcessor.parse(helper.getFilePath());
		wProcessor.determineTableRanges();
		Map<String, ExcelSheetPreProcessor> sProcessor = wProcessor.getSheetProcessors();

		ExcelSheetPreProcessor sheetProcessor = sProcessor.get(sheetName);
		List<ExcelBlock> blocks = sheetProcessor.getAllBlocks();
		for(ExcelBlock eBlock : blocks) {
			List<ExcelRange> ranges = eBlock.getRanges();
			for(ExcelRange eRange : ranges) {
				String range = eRange.getRangeSyntax();

				ExcelQueryStruct qs = new ExcelQueryStruct();
				qs.setSheetName(sheetName);
				System.out.println("Found range = " + range);
				qs.setSheetRange(range);
				
				System.out.println("Begin iterating ...");
				ExcelSheetFileIterator sheetIterator = helper.getSheetIterator(qs);
				while(sheetIterator.hasNext()) {
					IHeadersDataRow row = sheetIterator.next();
					Object[] values = row.getValues();
					String id = values[0].toString();
					String email = values[1].toString();
					
					if(email.contains("@")) {
						mappingToEmail.put(id, email);
					} else {
						System.out.println("INVALID EMAIL FOR ID=" + id + " and EMAIL=" + email);
					}
				}
			}
		}
		
		System.out.println("Ready to convert " + mappingToEmail.size() + " number of ids..." );
		
		System.out.println("Start by updating the form builder engine");

		// load the form builder engine
		{
			
			IEngine formEngine = new RDBMSNativeEngine();
			formEngine.setEngineId("form_builder_engine");
			formEngine.openDB(form_builder_engine_smss);
			DIHelper.getInstance().setLocalProperty("form_builder_engine", formEngine);
			
			// update the other tables
			String[] tables = new String[]{
					"EED10B32BC384718AB73C0C78480C174_FORM_LOG",
					"F4C4DEF02CED4F4D9C668EAB81B759B5_FORM_LOG",
					"FORMS_TAP_CORE_DATA_FORM_LOG",
					"FORMS_TAP_SITE_DATA_FORM_LOG",
					"FORMS_TAP_TEST_SITES_FORM_LOG",
					"_132DB94B43714763BFF9EDF7E5ED021B_FORM_LOG"
				};
			
			for(String table : tables) {
				// create index on user
				String index = "CREATE INDEX USERINDEX_" + table + " ON " + table + "(USER)";
				try {
					formEngine.insertData(index);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			for(String id : mappingToEmail.keySet()) {
				System.out.println("FROM BUILDER ENGINE : Updating " + id + " to " + mappingToEmail.get(id));
	
				String cleanId = RdbmsQueryBuilder.escapeForSQLStatement(id);
				String cleanEmail = RdbmsQueryBuilder.escapeForSQLStatement(mappingToEmail.get(id));
				
				String update = "UPDATE FORMS_USER_ACCESS SET USER_ID='" + cleanEmail + "' "
						+ "WHERE USER_ID='" + cleanId + "';";
				try {
					formEngine.insertData(update);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				for(String table : tables) {
					update = "UPDATE " + table + " SET USER='" + cleanEmail + "' WHERE USER='" + cleanId + "';";
					try {
						formEngine.insertData(update);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
			for(String table : tables) {
				// create index on user
				String index = "DROP INDEX USERINDEX_" + table;
				try {
					formEngine.insertData(index);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			System.out.println("Doing some manual clean up to remove 3 users (1 admin and 2 mistyped ids)");
			{
				String query = "DELETE FROM FORMS_USER_ACCESS WHERE USER_ID='15382338232';";
				try {
					formEngine.removeData(query);
				} catch (Exception e) {
					e.printStackTrace();
				}
				query = "DELETE FROM FORMS_USER_ACCESS WHERE USER_ID='15157649836';";
				try {
					formEngine.removeData(query);
				} catch (Exception e) {
					e.printStackTrace();
				}
				query = "DELETE FROM FORMS_USER_ACCESS WHERE USER_ID='1402072691';";
				try {
					formEngine.removeData(query);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			System.out.println("Done manual clean up");
			System.out.println("Done updating form builder engine");
		
		}

		System.out.println("Need to also update the forms tap core database");
		IEngine formsTapCore = new BigDataEngine();
		formsTapCore.setEngineId(forms_tap_core_id);
		formsTapCore.openDB(forms_tap_core_smss);
		DIHelper.getInstance().setLocalProperty("Forms_TAP_Core_Data", formsTapCore);
		
		{
			String query = "SELECT DISTINCT ?System (?System__CertificationUsername AS ?CertificationUsername) WHERE { "
					+ "{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}"
					+ "{?System <http://semoss.org/ontologies/Relation/Contains/CertificationUsername> ?System__CertificationUsername}"
					+ "}";
			
			Map<String, String> systemToId = new HashMap<String, String>();
			
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(formsTapCore, query);
			while(wrapper.hasNext()) {
				IHeadersDataRow header = wrapper.next();
				Object[] raw = header.getRawValues();
				Object[] row = header.getValues();
				
				systemToId.put(raw[0].toString(), row[1].toString());
			}
			
			// now we need to go through and update / delete
			
			for(String system : systemToId.keySet()) {
				String id = systemToId.get(system);
				
				// need to clean up the id to be an integer
				Double parsedDouble = null;
				try {
					parsedDouble = Double.parseDouble(id);
				} catch(Exception e) {
					continue;
				}
				
				Integer intValue = parsedDouble.intValue();
				String cleanId = intValue.toString();
				
				if(mappingToEmail.containsKey(cleanId)) {
					String newId = mappingToEmail.get(cleanId);

					System.out.println("FROM TAP CORE : Updating " + cleanId + " to " + newId);

					// due to bad input
					// we have things loaded as both strings and double
					// so will account for both
					formsTapCore.doAction(ACTION_TYPE.REMOVE_STATEMENT, 
							new Object[]{
									system, 
									"http://semoss.org/ontologies/Relation/Contains/CertificationUsername",
									cleanId,
									false});
					
					formsTapCore.doAction(ACTION_TYPE.REMOVE_STATEMENT, 
							new Object[]{
									system, 
									"http://semoss.org/ontologies/Relation/Contains/CertificationUsername",
									parsedDouble,
									false});
					
					
					formsTapCore.doAction(ACTION_TYPE.ADD_STATEMENT, 
							new Object[]{
									system, 
									"http://semoss.org/ontologies/Relation/Contains/CertificationUsername",
									newId,
									false});
				} else {
					System.out.println("ERROR ::: Could not identify new id for " + id + " using system = " + system);
				}
			}
			
			formsTapCore.commit();
		}
		
		System.out.println("Lastly, update the security database");
		IEngine securityDb = (IEngine) DIHelper.getInstance().getLocalProp("security");

		for(String id : mappingToEmail.keySet()) {
			System.out.println("SECURITY DATABASE : Updating " + id + " to " + mappingToEmail.get(id));

			String cleanId = RdbmsQueryBuilder.escapeForSQLStatement(id);
			String cleanEmail = RdbmsQueryBuilder.escapeForSQLStatement(mappingToEmail.get(id));
			
			String updateQuery = "UPDATE USER SET ID='" +  cleanEmail +"', EMAIL='" + cleanEmail + "' WHERE ID='" + cleanId + "'";
			try {
				securityDb.insertData(updateQuery);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// need to update all the places the user id is used
			updateQuery = "UPDATE ENGINEPERMISSION SET USERID='" +  cleanEmail +"' WHERE USERID='" + cleanId + "'";
			try {
				securityDb.insertData(updateQuery);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// need to update all the places the user id is used
			updateQuery = "UPDATE USERINSIGHTPERMISSION SET USERID='" +  cleanEmail +"' WHERE USERID='" + cleanId + "'";
			try {
				securityDb.insertData(updateQuery);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		////////////////////////////////////////////////////////////
		
		{
			// delete where user id is not a valid email
			IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(securityDb, "select distinct userid from enginepermission");
			while(manager.hasNext()) {
				String userId = manager.next().getValues()[0].toString();
				//  not an email
				if(!userId.contains("@")) {
					String delete = "DELETE FROM ENGINEPERMISSION WHERE USERID='" + userId + "'";
					try {
						securityDb.insertData(delete);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			
			manager = WrapperManager.getInstance().getRawWrapper(securityDb, "select distinct id from user");
			while(manager.hasNext()) {
				String userId = manager.next().getValues()[0].toString();
				//  not an email
				if(!userId.contains("@")) {
					String delete = "DELETE FROM USER WHERE ID='" + userId + "'";
					try {
						securityDb.insertData(delete);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		securityDb.commit();
		System.out.println("Done with updates");
	}

}
