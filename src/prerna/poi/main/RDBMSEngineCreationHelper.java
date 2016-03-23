package prerna.poi.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RDBMSEngineCreationHelper {

	private RDBMSEngineCreationHelper() {
		
	}
	
	public static void writeDefaultQuestionSheet(IEngine rdbmsEngine)
	{		
//		QuestionAdministrator questionAdmin = new QuestionAdministrator(((AbstractEngine)rdbmsEngine));
		
		String engineName = rdbmsEngine.getEngineName();
		// get all the tables names in the database
		String getAllTablesQuery = "SHOW TABLES FROM PUBLIC";
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(rdbmsEngine, getAllTablesQuery);
		String[] names = wrapper.getVariables();
		Set<String> tableNames = new HashSet<String>();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String tableName = ss.getVar("TABLE_NAME") + "";
			tableNames.add(tableName);
		}
		
		// file location
		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
		String fileName = dbBaseFolder + System.getProperty("file.separator") + "db" + System.getProperty("file.separator") + engineName + System.getProperty("file.separator") + engineName + "_Questions.properties";

		Properties prop = new Properties();
		prop.put("PERSPECTIVE", "Generic-Perspective");
		String genericQueries = "GQ0";
		String questionName = "Explore a concept from the database";
		String exploreQuery = "SELECT X.@Concept-Concept:Concept@ AS @Concept-Concept:Concept@ "
				+ "From @Concept-Concept:Concept@ X WHERE X.@Concept-Concept:Concept@='@Instance-Instance:Instance@'";
		String conceptParamQuery = "SELECT DISTINCT (COALESCE(?DisplayName, ?PhysicalName) AS ?entity) WHERE "
				+ " { {?PhysicalName <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} "
				+ " FILTER (?PhysicalName != <http://semoss.org/ontologies/Concept>) "
				+ " OPTIONAL{?PhysicalName <http://semoss.org/ontologies/DisplayName> ?DisplayName } }";
		String instanceParamQuery = "SELECT Distinct @Concept@ FROM @Concept@";
		
		prop.put("GQ0", questionName);
		prop.put("GQ0" + "_LAYOUT", "Graph");
		prop.put("GQ0" + "_QUERY", exploreQuery);
		prop.put("GQ0_Instance_DEPEND", "Concept");
		prop.put("GQ0_Concept_QUERY", conceptParamQuery);
		prop.put("GQ0_Concept_DB_QUERY", "FALSE");
		prop.put("GQ0_Instance_QUERY", instanceParamQuery);
		
//		List<DataMakerComponent> dmcList = new ArrayList<DataMakerComponent>();
//		DataMakerComponent dmc = new DataMakerComponent(rdbmsEngine, exploreQuery);
//		dmcList.add(dmc);
//		
//		List<SEMOSSParam> params = new ArrayList<SEMOSSParam>();
//		SEMOSSParam p1 = new SEMOSSParam();
//		p1.setName("Concept");
//		p1.setDbQuery(false);
//		p1.setQuery(conceptParamQuery);
//		params.add(p1);
//		SEMOSSParam p2 = new SEMOSSParam();
//		p2.setName("Instance");
//		p2.setDbQuery(true);
//		p2.setDepends("Concept");
//		p2.setQuery(instanceParamQuery);
//		params.add(p2);
//
//		questionAdmin.addQuestion(questionName, "Generic-Perspective", dmcList, "Graph", "0", "GraphDataModel", true, null, params, null);
		
		int questionOrder = 1;
		String layout = "Grid";
		for(String tableName : tableNames)
		{
			genericQueries += ";" + "GQ" + questionOrder;			
			questionName = "Show all from " + tableName;
			String sql = "SELECT * FROM " + tableName;
			prop.put("GQ" + questionOrder, questionName);
			prop.put("GQ" + questionOrder + "_LAYOUT", layout);
			prop.put("GQ" + questionOrder + "_QUERY", sql);
			
//			dmcList = new ArrayList<DataMakerComponent>();
//			dmc = new DataMakerComponent(rdbmsEngine, sql);
//			dmcList.add(dmc);
//			questionAdmin.addQuestion(questionName, "Generic-Perspective", dmcList, layout, questionOrder + "", "TinkerFrame", true, null, null, null);
			
			questionOrder++;
		}
		prop.put("Generic-Perspective", genericQueries);

		FileOutputStream fo = null;
		try {
			File file = new File(fileName);
			fo = new FileOutputStream(file);
			prop.store(fo, "Questions for RDBMS");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fo != null) {
				try {
					fo.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
}
