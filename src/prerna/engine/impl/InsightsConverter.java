package prerna.engine.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.h2.store.fs.FileUtils;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.H2QueryUtil;
import prerna.util.sql.SQLQueryUtil;

public class InsightsConverter {

	private static final String QUESITON_QUERY = "SELECT DISTINCT "
			+ "?INSIGHT_KEY ?INSIGHT_NAME ?PERSPECTIVE ?QUERY ?LAYOUT ?ORDER WHERE { "
			+ "{?INSIGHT_KEY a <http://semoss.org/ontologies/Concept/Insight>} "
			+ "{?INSIGHT_KEY <http://semoss.org/ontologies/Relation/Contains/SPARQL> ?QUERY} "
			+ "{?INSIGHT_KEY <http://semoss.org/ontologies/Relation/Contains/Layout> ?LAYOUT} "
			+ "{?PERSPECTIVE <http://semoss.org/ontologies/Relation/Perspective:Insight> ?INSIGHT_KEY} "
			+ "{?INSIGHT_KEY <http://semoss.org/ontologies/Relation/Contains/Label> ?INSIGHT_NAME} "
			+ "{?INSIGHT_KEY <http://semoss.org/ontologies/Relation/Contains/Order> ?ORDER} "
			+ "} ORDER BY ?INSIGHT_KEY";
	
	private static final String PARAMETER_QUERY = "SELECT DISTINCT "
			+ "?INSIGHT_KEY ?PARAM_ID ?PARAM_NAME ?PARAM_TYPE ?PARAM_DEPENDENCY ?PARAM_QUERY ?PARAM_OPTIONS WHERE {"
			+ "{?INSIGHT_KEY <INSIGHT:PARAM> ?PARAM_ID} "
			+ "{?PARAM_ID <PARAM:LABEL> ?PARAM_NAME} "
			+ "{?PARAM_ID <PARAM:TYPE> ?PARAM_TYPE} "
			+ "{?PARAM_ID <PARAM:DEPEND> ?PARAM_DEPENDENCY} "
			+ "OPTIONAL {?PARAM_ID <PARAM:QUERY> ?PARAM_QUERY} "
			+ "OPTIONAL {?PARAM_ID <PARAM:OPTION> ?PARAM_OPTIONS}"
			+ "} ORDER BY ?INSIGHT_KEY ?PARAM_DEPENDENCY";
	
	private Map<String, String> newQuestionIDMap = new HashMap<String, String>();
	
	private String xmlPath;
	private String baseFolder;
	private String smssLocation;
	private String engineName;
	private RDFFileSesameEngine insightBaseXML;
	private RDBMSNativeEngine insightRDBMSEngine;
	private SQLQueryUtil queryUtil;

	public static void main(String[] args) throws Exception {
		Properties prop = new Properties();
		prop.setProperty("BaseFolder", "C:/workspace/Semoss/db");
		DIHelper.getInstance().setCoreProp(prop);
		InsightsConverter test = new InsightsConverter();
		//set paths
		String xmlFilePath = "C:\\workspace\\Semoss\\db\\Movie_DB\\Movie_DB_Questions.XML";
		test.createXMLEngine(xmlFilePath);
		String baseFolder = "C:/workspace/Semoss/db";
		test.setBaseFolder(baseFolder);
		String engineName = "Movie_DB";
		test.setEngineName(engineName);
		String smssLocation = "C:\\workspace\\Semoss\\db\\Movie_DB.smss";
		test.setSMSSLocation(smssLocation);
		
		//create rdbms engine
		test.generateNewInsightsRDBMS();
		//query xml file
		test.processQuestionTableFromQuery();
		test.processParameterTableFromQuery();
		//update smss location
		test.updateSMSSFile();
	}

	public void processQuestionTableFromQuery() {
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(insightBaseXML, QUESITON_QUERY);
		String[] names = wrapper.getVariables();
		int counter = 1;
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String INSIGHT_KEY = ss.getVar(names[0]) + "";
			String INSIGHT_NAME = ss.getVar(names[1]) + "";
			String PERSPECTIVE = ss.getVar(names[2]).toString().split(":")[1];
			String QUERY = ss.getVar(names[3]) + "";
			String LAYOUT = ss.getVar(names[4]) + "";
			String ORDER = ss.getVar(names[5]) + "";

			String NEW_INSIGHT_KEY = engineName + "_" + counter;
			newQuestionIDMap.put(INSIGHT_KEY, NEW_INSIGHT_KEY);
			
			String QUERY_MAKEUP = generateQueryMakeUp(engineName, QUERY);
			
			String query = "INSERT INTO QUESTION_ID (QUESTION_ID, QUESTION_NAME, QUESTION_PERSPECTIVE, QUESTION_MAKEUP, QUESTION_LAYOUT, QUESTION_ORDER) VALUES("
					+ "'" + NEW_INSIGHT_KEY + "', "
					+ "'" + INSIGHT_NAME + "', "
					+ "'" + PERSPECTIVE + "', "
					+ "'" + QUERY_MAKEUP + "', "
					+ "'" + LAYOUT + "', "
					+ ORDER + ")";
			
			insightRDBMSEngine.insertData(query);
			counter++;
//			System.out.println(NEW_INSIGHT_KEY + "|" + INSIGHT_NAME + "|" + PERSPECTIVE + "|" + QUERY + "|" + LAYOUT + "|" + ORDER);
		}
	}
	
	public void processParameterTableFromQuery() {
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(insightBaseXML, PARAMETER_QUERY);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement ss = wrapper.next();
			String INSIGHT_KEY = ss.getVar(names[0]) + "";
//			String PARAM_ID = ss.getVar(names[1]) + "";
			String PARAM_NAME = ss.getVar(names[2]) + "";
			String PARAM_TYPE = ss.getRawVar(names[3]) + "";
			String PARAM_DEPENDENCY = ss.getVar(names[4]) + "";
			String PARAM_QUERY = ss.getVar(names[5]) + "";
			String PARAM_OPTIONS = ss.getVar(names[6]) + "";

			String QUESTION_ID_FK = newQuestionIDMap.get(INSIGHT_KEY);
			String NEW_PARAM_ID = QUESTION_ID_FK + "_" + PARAM_NAME;
			
//			String QUERY_MAKEUP = "";
//			if(!PARAM_QUERY.isEmpty()) {
//				QUERY_MAKEUP = generateQueryMakeUp(engineName, PARAM_QUERY);
//			}
			
			String query = "INSERT INTO PARAMETER_ID (PARAMETER_ID, PARAMETER_LABEL, PARAMETER_TYPE, PARAMETER_DEPENDENCY, PARAMETER_MAKEUP, PARAMETER_OPTIONS, QUESTION_ID_FK) VALUES("
					+ "'" + NEW_PARAM_ID + "', "
					+ "'" + PARAM_NAME + "', "
					+ "'" + PARAM_TYPE + "', "
					+ "'" + PARAM_DEPENDENCY + "', "
					+ "'" + PARAM_QUERY + "', "
					+ "'" + PARAM_OPTIONS + "', "
					+ "'" + QUESTION_ID_FK + "')";
			
			insightRDBMSEngine.insertData(query);

//			System.out.println(INSIGHT_KEY + "|" + PARAM_ID + "|" + PARAM_NAME + "|" + PARAM_TYPE + "|" + PARAM_DEPENDENCY + "|" + PARAM_QUERY + "|" + PARAM_OPTIONS);
		}
	}
	
	public void generateNewInsightsRDBMS() {
		String dbProp = writePropFile(engineName);
		insightRDBMSEngine = new RDBMSNativeEngine();
		insightRDBMSEngine.openDB(dbProp);
		generateTables();
		
		FileUtils.delete(dbProp);
	}
	
	private void generateTables() {
		String questionTableCreate = "CREATE TABLE QUESTION_ID ("
				+ "QUESTION_ID VARCHAR(255), "
				+ "QUESTION_NAME VARCHAR(255), "
				+ "QUESTION_PERSPECTIVE VARCHAR(225), "
				+ "QUESTION_LAYOUT VARCHAR(225), "
				+ "QUESTION_ORDER INT, "
				+ "QUESTION_MAKEUP CLOB)";
		
		insightRDBMSEngine.insertData(questionTableCreate);
		
		String parameterTableCreate = "CREATE TABLE PARAMETER_ID ("
				+ "PARAMETER_ID VARCHAR(255), "
				+ "PARAMETER_LABEL VARCHAR(255), "
				+ "PARAMETER_TYPE VARCHAR(225), "
				+ "PARAMETER_DEPENDENCY VARCHAR(225), "
				+ "PARAMETER_MAKEUP VARCHAR(225), "
				+ "PARAMETER_OPTIONS VARCHAR(225), "
				+ "QUESTION_ID_FK VARCHAR(225))";
		
		insightRDBMSEngine.insertData(parameterTableCreate);
	}
	
	private String generateQueryMakeUp(String engine, String query) {
		String makeup = "<http://semoss.org/ontologies/Concept/Engine> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> .\n"
				+ "<http://semoss.org/ontologies/Concept/Engine/" + engineName + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Engine> .\n"
				+ "<http://semoss.org/ontologies/Concept/QueryNum> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept> .\n"
				+ "<http://semoss.org/ontologies/Concept/QueryNum/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/QueryNum> .\n"
				+ "<http://semoss.org/ontologies/Concept/Engine/" + engineName + "> <http://semoss.org/ontologies/Relation/Has> <http://semoss.org/ontologies/Concept/QueryNum/1> .\n"
				+ "<http://semoss.org/ontologies/Concept/QueryNum/1> <http://semoss.org/ontologies/Relation/Contains/Query> \"" + query + "\" .\n";
		return makeup;
	}

	public String writePropFile(String engineName) {
		queryUtil = new H2QueryUtil();
		Properties prop = new Properties();
		String connectionURL = "jdbc:h2:" + baseFolder + System.getProperty("file.separator") + engineName + System.getProperty("file.separator") + 
				"database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		prop.put(Constants.CONNECTION_URL, connectionURL);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER,queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.TEMP_CONNECTION_URL, queryUtil.getTempConnectionURL());
		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());
		prop.put("TEMP", "TRUE");

		// write this to a file
		String tempFile = baseFolder + "/" + engineName + "/conn.prop";
		
		File file = null;
		FileOutputStream fo = null;
		try {
			file = new File(tempFile);
			fo = new FileOutputStream(file);
			prop.store(fo, "Temporary Properties file for the RDBMS");
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
		return tempFile;
	}
	
	public void updateSMSSFile() {
		FileOutputStream fileOut = null;
		File file = new File(smssLocation);
		List<String> content = new ArrayList<String>();
		
		BufferedReader reader = null;
		FileReader fr = null;
		String locInFile = "ENGINE_TYPE";
		try{
			fr = new FileReader(file);
			reader = new BufferedReader(fr);
			String line;
			while((line = reader.readLine()) != null){
				content.add(line);
			}
			
			fileOut = new FileOutputStream(file);
			for(int i=0; i<content.size(); i++){
				byte[] contentInBytes = content.get(i).getBytes();
				fileOut.write(contentInBytes);
				fileOut.write("\n".getBytes());
				
				if(content.get(i).contains(locInFile)){
					String newProp = Constants.RDBMS_INSIGHTS + "\t" + "db/" + engineName + "/database";
					fileOut.write(newProp.getBytes());
					fileOut.write("\n".getBytes());
				}
			}
			
		} catch(IOException e){
			e.printStackTrace();
		} finally{
			try{
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			try{
				fileOut.close();
			} catch (IOException e){
				e.printStackTrace();
			}
		}
	}
	
	public void createXMLEngine(String xmlPath) {
		this.xmlPath = xmlPath;
		this.insightBaseXML = new RDFFileSesameEngine();
		insightBaseXML.setFileName(xmlPath);
		insightBaseXML.openDB(null);
	}
	
	public void setBaseFolder(String baseFolder) {
		this.baseFolder = baseFolder;
	}
	
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}
	
	public void setSMSSLocation(String smssLocation) {
		this.smssLocation = smssLocation;
	}
	
	public RDBMSNativeEngine getInsightRDBMSEngine() {
		return this.insightRDBMSEngine;
	}
}
