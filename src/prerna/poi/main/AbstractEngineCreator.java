package prerna.poi.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.rdbms.ImpalaEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.sql.H2QueryUtil;
import prerna.util.sql.SQLQueryUtil;

public class AbstractEngineCreator {

	private static final Logger logger = LogManager.getLogger(AbstractEngineCreator.class.getName());

	protected IEngine engine;
	// OWL variables
	protected String owlFile = "";
	protected OWLER owler;
	
	protected String semossURI;
	protected String customBaseURI = "";
	protected String dbPropFile;

	// sadly need to keep RDBMS specific object
	protected SQLQueryUtil queryUtil;
	// keep conversion from user input to sql datatypes
	protected Map<String, String> sqlHash = new Hashtable<String, String>();
	
	protected void prepEngineCreator(String customBase, String owlFile, String dbPropFile) {
		//make location of the owl file in the dbname folder
		this.owlFile = owlFile; 
		// location of dbPropFile
		this.dbPropFile = dbPropFile;
		semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
		if(customBase != null && !customBase.equals("")) {
			customBaseURI = customBase.trim();
		} else {
			customBaseURI = semossURI;
		}
	}
	
	protected void openRdfEngineWithoutConnection(String dbName) {
		createNewRdfEngine(dbName);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.SESAME, this.customBaseURI);
	}
	
	protected void openRdbmsEngineWithoutConnection(String dbName) {
		createNewRDBMSEngine(dbName);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.RDBMS, this.customBaseURI);
	}
	
	protected void openTinkerEngineWithoutConnection(String dbName) {
		createNewTinkerEngine(dbName);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.SESAME, this.customBaseURI);
	}
	
	private void createNewRDBMSEngine(String dbName) {
		engine = new RDBMSNativeEngine();
		engine.setEngineName(dbName);
		Properties prop = new Properties();
		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
		prop.put(Constants.CONNECTION_URL, queryUtil.getConnectionURL(dbBaseFolder,dbName));
		prop.put(Constants.ENGINE, dbName);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER,queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());
		prop.put("TEMP", "TRUE");
		((AbstractEngine) engine).setProperties(prop);
		engine.openDB(null);
		
		// create the insight database
		IEngine insightDatabase = createNewInsightsDatabase(dbName);
		engine.setInsightDatabase(insightDatabase);
	}

	private void createNewRdfEngine(String dbName) {
		engine = new BigDataEngine();
		engine.setEngineName(dbName);
		engine.openDB(dbPropFile);
		
		String sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
		String typeOf = RDF.TYPE.stringValue();
		String obj = Constants.CLASS_URI;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, typeOf, obj, true});
		
		sub =  semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		obj = Constants.DEFAULT_PROPERTY_URI;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, typeOf, obj, true});
		
		// create the insight database
		IEngine insightDatabase = createNewInsightsDatabase(dbName);
		engine.setInsightDatabase(insightDatabase);
	}
	
	private void createNewTinkerEngine(String dbName) {
		engine = new TinkerEngine();
		engine.setEngineName(dbName);
		engine.openDB(dbPropFile);
		
		// create the insight database
		IEngine insightDatabase = createNewInsightsDatabase(dbName);
		engine.setInsightDatabase(insightDatabase);
	}
	
	//added for connect to external RDBMS workflow
	protected void generateEngineFromRDBMSConnection(String schema, String dbName) {
		connectToExternalRDBMSEngine(schema,dbName);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.RDBMS, this.customBaseURI);
	}
	
	//added for connect to external Impala workflow
	protected void generateEngineFromImpalaConnection(String schema, String dbName) {
		connectToExternalImpalaEngine(schema,dbName);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.IMPALA, this.customBaseURI);
	}
	//added for connect to external RDBMS workflow
	private void connectToExternalRDBMSEngine(String schema, String dbName) {
		engine = new RDBMSNativeEngine();
		engine.setEngineName(dbName);
		Properties prop = new Properties();
		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
		prop.put(Constants.CONNECTION_URL, queryUtil.getConnectionURL(dbBaseFolder,dbName));
		prop.put(Constants.ENGINE, dbName);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER, queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.RDBMS_TYPE, queryUtil.getDatabaseType().toString());
		
		// setting as a parameter for engine
		//prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + dbName + System.getProperty("file.separator") + "insights_database");
		prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + "@engine@" + System.getProperty("file.separator") + "insights_database");
		prop.put("TEMP", "TRUE");
		prop.put("SCHEMA", schema);//schema comes from existing db (connect to external db(schema))
		((AbstractEngine) engine).setProperties(prop);
		engine.openDB(null);
		
		// create the insight database
		IEngine insightDatabase = createNewInsightsDatabase(dbName);
		engine.setInsightDatabase(insightDatabase);
	}

	//added for connect to external Impala workflow
	private void connectToExternalImpalaEngine(String schema, String dbName) {
		engine = new ImpalaEngine();
		engine.setEngineName(dbName);
		Properties prop = new Properties();
		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
		prop.put(Constants.CONNECTION_URL, queryUtil.getConnectionURL(dbBaseFolder,dbName));
		prop.put(Constants.ENGINE, dbName);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER,queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());

		// setting as a parameter for engine
		//prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + dbName + System.getProperty("file.separator") + "insights_database");
		prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + "@engine@" + System.getProperty("file.separator") + "insights_database");
		prop.put("TEMP", "TRUE");
		prop.put("SCHEMA", schema);//schema comes from existing db (connect to external db(schema))
		((AbstractEngine) engine).setProperties(prop);
		engine.openDB(null);

		// create the insight database
		IEngine insightDatabase = createNewInsightsDatabase(dbName);
		engine.setInsightDatabase(insightDatabase);
	}

	protected void openEngineWithConnection(String engineName) {
		engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		openOWLWithConnection(engine, owlFile);
	}
	
	/**
	 * Close the database engine
	 * @throws IOException 
	 */
	protected void closeDB() throws IOException {
		logger.warn("Closing....");
		if(engine != null) {
			commitDB();
			engine.closeDB();
		}
	}	

	protected void commitDB() throws IOException {
		logger.warn("Committing....");
		// commit the created engine
		engine.commit();
		// also commit the created insights rdbms engine
		engine.getInsightDatabase().commit();
		
		if(engine!=null && engine instanceof BigDataEngine){
			((BigDataEngine)engine).infer();
		} else if(engine!=null && engine instanceof RDFFileSesameEngine){
			try {
				((RDFFileSesameEngine)engine).exportDB();
			} catch (RepositoryException | RDFHandlerException | IOException e) {
				e.printStackTrace();
				throw new IOException("Unable to commit data from file into database");
			}
		}
	}

	/**
	 * Creates a repository connection to be put all the base relationship data
	 * to create the OWL file
	 * 
	 * @throws EngineException
	 */
	protected void openOWLWithOutConnection(String owlFile, IEngine.ENGINE_TYPE type, String customBaseURI) {
		owler = new OWLER(owlFile, type);
		owler.addCustomBaseURI(customBaseURI);
	}

	/**
	 * Creates a repository connection and puts all the existing base
	 * relationships to create an updated OWL file
	 * 
	 * @param engine
	 *            The database engine used to get all the existing base
	 *            relationships
	 * @throws EngineException
	 */
	protected void openOWLWithConnection(IEngine engine, String owlFile) {
		owler = new OWLER(engine, owlFile);
	}

	/**
	 * Close the OWL engine
	 * @throws EngineException
	 */
	protected void closeOWL() {
		owler.closeOwl();
	}
	
	/**
	 * Writes the base information in the OWL to a file
	 * @throws IOException 
	 */
	protected void createBaseRelations() throws IOException {
		owler.commit();
		try {
			owler.export();
		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Unable to export OWL file...");
		}
	}
	
	/**
	 * Fill in the sqlHash with the types
	 */
	protected void createSQLTypes() {
		sqlHash.put("DECIMAL", "FLOAT");
		sqlHash.put("DOUBLE", "FLOAT");
		sqlHash.put("STRING", "VARCHAR(2000)"); // 8000 was chosen because this is the max for SQL Server; needs more permanent fix
		sqlHash.put("TEXT", "VARCHAR(2000)"); // 8000 was chosen because this is the max for SQL Server; needs more permanent fix
		//TODO: the FE needs to differentiate between "dates with times" vs. "dates"
		sqlHash.put("DATE", "DATE");
		sqlHash.put("SIMPLEDATE", "DATE");
		// currently only add in numbers as doubles
		sqlHash.put("NUMBER", "FLOAT");
		sqlHash.put("INTEGER", "FLOAT");
		sqlHash.put("BOOLEAN", "BOOLEAN");
	}
	
	/**
	 * Creates the default insights rdbms engine
	 * Also adds the explore an instance query
	 * @param engineName
	 * @return
	 */
	protected IEngine createNewInsightsDatabase(String engineName) {
		H2QueryUtil queryUtil = new H2QueryUtil();
		Properties prop = new Properties();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String connectionURL = "jdbc:h2:" + baseFolder + System.getProperty("file.separator") + "db" + System.getProperty("file.separator") + engineName + System.getProperty("file.separator") + 
				"insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		prop.put(Constants.CONNECTION_URL, connectionURL);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER,queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());
		prop.put("TEMP", "TRUE");
		RDBMSNativeEngine insightRDBMSEngine = new RDBMSNativeEngine();
		insightRDBMSEngine.setProperties(prop);
		// opening will work since we directly injected the prop map
		// this way i do not need to write it to disk and then recreate it later
		insightRDBMSEngine.openDB(null);
		
		// CREATE TABLE QUESTION_ID (ID VARCHAR(50), QUESTION_NAME VARCHAR(255), QUESTION_PERSPECTIVE VARCHAR(225), QUESTION_LAYOUT VARCHAR(225), QUESTION_ORDER INT, QUESTION_DATA_MAKER VARCHAR(225), QUESTION_MAKEUP CLOB, QUESTION_PROPERTIES CLOB, QUESTION_OWL CLOB, QUESTION_IS_DB_QUERY BOOLEAN, DATA_TABLE_ALIGN VARCHAR(500), QUESTION_PKQL ARRAY)
		String questionTableCreate = "CREATE TABLE QUESTION_ID ("
				+ "ID VARCHAR(50), "
				+ "QUESTION_NAME VARCHAR(255), "
				+ "QUESTION_PERSPECTIVE VARCHAR(225), "
				+ "QUESTION_LAYOUT VARCHAR(225), "
				+ "QUESTION_ORDER INT, "
				+ "QUESTION_DATA_MAKER VARCHAR(225), "
				+ "QUESTION_MAKEUP CLOB, "
				+ "QUESTION_PROPERTIES CLOB, "
				+ "QUESTION_OWL CLOB, "
				+ "QUESTION_IS_DB_QUERY BOOLEAN, "
				+ "DATA_TABLE_ALIGN VARCHAR(500), "
				+ "HIDDEN_INSIGHT BOOLEAN, "
				+ "QUESTION_PKQL ARRAY)";

		insightRDBMSEngine.insertData(questionTableCreate);

		// CREATE TABLE PARAMETER_ID (PARAMETER_ID VARCHAR(255), PARAMETER_LABEL VARCHAR(255), PARAMETER_TYPE VARCHAR(225), PARAMETER_DEPENDENCY VARCHAR(225), PARAMETER_QUERY VARCHAR(2000), PARAMETER_OPTIONS VARCHAR(2000), PARAMETER_IS_DB_QUERY BOOLEAN, PARAMETER_MULTI_SELECT BOOLEAN, PARAMETER_COMPONENT_FILTER_ID VARCHAR(255), PARAMETER_VIEW_TYPE VARCHAR(255), QUESTION_ID_FK INT)
		String parameterTableCreate = "CREATE TABLE PARAMETER_ID ("
				+ "PARAMETER_ID VARCHAR(255), "
				+ "PARAMETER_LABEL VARCHAR(255), "
				+ "PARAMETER_TYPE VARCHAR(225), "
				+ "PARAMETER_DEPENDENCY VARCHAR(225), "
				+ "PARAMETER_QUERY VARCHAR(2000), "
				+ "PARAMETER_OPTIONS VARCHAR(2000), "
				+ "PARAMETER_IS_DB_QUERY BOOLEAN, "
				+ "PARAMETER_MULTI_SELECT BOOLEAN, "
				+ "PARAMETER_COMPONENT_FILTER_ID VARCHAR(255), "
				+ "PARAMETER_VIEW_TYPE VARCHAR(255), "
				+ "QUESTION_ID_FK INT)";

		insightRDBMSEngine.insertData(parameterTableCreate);
		
		String feTableCreate = "CREATE TABLE UI ("
				+ "QUESTION_ID_FK INT, "
				+ "UI_DATA CLOB)";
		
		insightRDBMSEngine.insertData(feTableCreate);
		
		// let us automatically add the explore an instance query
		InsightAdministrator admin = new InsightAdministrator(insightRDBMSEngine);
		String insightName = "Explore an instance of a selected node type";
		String layout = "Graph";
		String exploreLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\ExploreInstanceDefaultWidget.json";
		File exploreF = new File(exploreLoc);
		if(exploreF.exists()) {
			String newPixel = "AddPanel(0); Panel ( 0 ) | SetPanelView ( \"param\" , \"<encode> {\"json\":";
			try {
				newPixel += new String(Files.readAllBytes(exploreF.toPath()))
						.replaceAll("\n|\r|\t", "")
						.replaceAll("\\s\\s+", "")
						.replace("<<ENGINE>>", engineName);
				newPixel += "} </encode>\" ) ;";
				String[] pkqlRecipeToSave = {newPixel};
				admin.addInsight(insightName, layout, pkqlRecipeToSave);
				insightRDBMSEngine.commit();
			} catch (IOException e2) {
				logger.error("Error creating the explore an instance query!");
			}
		}
		return insightRDBMSEngine;
	}
}
