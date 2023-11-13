//package prerna.poi.main;
//
//import java.io.IOException;
//import java.util.Hashtable;
//import java.util.Map;
//import java.util.Properties;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.openrdf.model.vocabulary.RDF;
//
//import prerna.algorithm.api.SemossDataType;
//import prerna.engine.api.IDatabaseEngine;
//import prerna.engine.api.impl.util.Owler;
//import prerna.engine.impl.r.RNativeEngine;
//import prerna.engine.impl.rdbms.RDBMSNativeEngine;
//import prerna.engine.impl.rdf.BigDataEngine;
//import prerna.engine.impl.rdf.RDFFileSesameEngine;
//import prerna.engine.impl.tinker.TinkerEngine;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.Utility;
//import prerna.util.sql.AbstractSqlQueryUtil;
//import prerna.util.sql.RDBMSUtility;
//import prerna.util.sql.RdbmsTypeEnum;
//
//public class AbstractEngineCreator {
//
//	private static final Logger logger = LogManager.getLogger(AbstractEngineCreator.class.getName());
//
//	protected IDatabaseEngine engine;
//	// OWL variables
//	protected String owlFile = "";
//	protected Owler owler;
//	
//	protected String semossURI;
//	protected String customBaseURI = "";
//	protected String smssPropFile;
//
//	// sadly need to keep RDBMS specific object
//	protected AbstractSqlQueryUtil queryUtil;
//	// keep conversion from user input to sql datatypes
//	protected Map<String, String> sqlHash = new Hashtable<String, String>();
//	
//	protected void prepEngineCreator(String customBase, String owlFile, String smssPropFile) {
//		//make location of the owl file in the dbname folder
//		this.owlFile = owlFile; 
//		// location of dbPropFile
//		this.smssPropFile = smssPropFile;
//		semossURI = DIHelper.getInstance().getProperty(Constants.SEMOSS_URI);
//		if(customBase != null && !customBase.equals("")) {
//			customBaseURI = customBase.trim();
//		} else {
//			customBaseURI = semossURI;
//		}
//	}
//	
//	protected void openRdfEngineWithoutConnection(String engineName, String engineId) throws Exception {
//		createNewRdfEngine(engineName, engineId);
//		openOWLWithOutConnection(engineId, owlFile, IDatabaseEngine.DATABASE_TYPE.SESAME, this.customBaseURI);
//	}
//	
//	protected void openRdbmsEngineWithoutConnection(String engineName, String engineId) throws Exception {
//		createNewRDBMSEngine(engineName, engineId);
//		openOWLWithOutConnection(engineId, owlFile, IDatabaseEngine.DATABASE_TYPE.RDBMS, this.customBaseURI);
//	}
//	
//	protected void openTinkerEngineWithoutConnection(String engineName, String engineId) throws Exception {
//		createNewTinkerEngine(engineName, engineId);
//		openOWLWithOutConnection(engineId, owlFile, IDatabaseEngine.DATABASE_TYPE.SESAME, this.customBaseURI);
//	}
//	
//	protected void openREngineWithoutConnection(String engineName, String engineId) throws Exception {
//		createNewREngine(engineName, engineId);
//		openOWLWithOutConnection(engineId, owlFile, IDatabaseEngine.DATABASE_TYPE.SESAME, this.customBaseURI);
//	}
//	
//	private void createNewRDBMSEngine(String engineName, String engineId) throws Exception {
//		engine = new RDBMSNativeEngine();
//		engine.setEngineId(engineId);
//		engine.setEngineName(engineName);
//		Properties prop = new Properties();
////		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
////		prop.put(Constants.CONNECTION_URL, queryUtil.getConnectionURL(dbBaseFolder,SmssUtilities.getUniqueName(engineName, engineId)));
//		// open db will fill in the parameterization for us!
//		if(this.queryUtil == null || this.queryUtil.getConnectionUrl() == null || this.queryUtil.getConnectionUrl().isEmpty()) {
//			prop.put(Constants.CONNECTION_URL, RDBMSUtility.getH2BaseConnectionURL2());
//			prop.put(Constants.USERNAME, "sa");
//			prop.put(Constants.PASSWORD, "");
//			prop.put(Constants.RDBMS_TYPE, RdbmsTypeEnum.H2_DB.toString());
//		} else {
//			prop.put(Constants.CONNECTION_URL, this.queryUtil.getConnectionUrl());
//			prop.put(Constants.USERNAME, queryUtil.getUsername());
//			prop.put(Constants.PASSWORD, queryUtil.getPassword());
//			prop.put(Constants.DRIVER, queryUtil.getDriver());
//			prop.put(Constants.RDBMS_TYPE, queryUtil.getDbType().toString());
//		}
//		prop.put(Constants.ENGINE, engineId);
//		prop.put(Constants.ENGINE_ALIAS, engineName);
//		prop.put("TEMP", "TRUE");
//		engine.open(prop);
//	}
//
//	private void createNewRdfEngine(String engineName, String engineId) throws Exception {
//		engine = new BigDataEngine();
//		engine.setEngineId(engineId);
//		engine.setEngineName(engineName);
//		engine.open(smssPropFile);
//		
//		String sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
//		String typeOf = RDF.TYPE.stringValue();
//		String obj = Constants.CLASS_URI;
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, typeOf, obj, true});
//		
//		sub =  semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
//		obj = Constants.DEFAULT_PROPERTY_URI;
//		engine.doAction(IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, typeOf, obj, true});
//		
//	}
//	
//	private void createNewTinkerEngine(String engineName, String engineId) throws Exception {
//		engine = new TinkerEngine();
//		engine.setEngineId(engineId);
//		engine.setEngineName(engineName);
//		engine.open(smssPropFile);
//		
//	}
//	
//	private void createNewREngine(String engineName, String engineId) throws Exception {
//		engine = new RNativeEngine();
//		engine.setEngineName(engineName);
//		engine.setEngineId(engineId);
//		engine.open(smssPropFile);
//	}
//	
//	//added for connect to external RDBMS workflow
//	protected void generateEngineFromRDBMSConnection(String schema, String engineName, String engineId) throws Exception {
//		connectToExternalRDBMSEngine(schema,engineName, engineId);
//		openOWLWithOutConnection(engineId, owlFile, IDatabaseEngine.DATABASE_TYPE.RDBMS, this.customBaseURI);
//	}
//	
////	//added for connect to external Impala workflow
////	protected void generateEngineFromImpalaConnection(String schema, String dbName, String engineId) throws Exception {
////		connectToExternalImpalaEngine(schema,dbName, engineId);
////		openOWLWithOutConnection(owlFile, IDatabase.ENGINE_TYPE.IMPALA, this.customBaseURI);
////	}
//	
//	//added for connect to external RDBMS workflow
//	private void connectToExternalRDBMSEngine(String schema, String engineName, String engineId) throws Exception {
//		engine = new RDBMSNativeEngine();
//		engine.setEngineId(engineId);
//		engine.setEngineName(engineName);
//		Properties prop = new Properties();
////		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
////		prop.put(Constants.CONNECTION_URL, queryUtil.getConnectionURL(dbBaseFolder, SmssUtilities.getUniqueName(engineName, engineId)));
//		// grab from the query util 
//		prop.put(Constants.CONNECTION_URL, this.queryUtil.getConnectionUrl());
//		prop.put(Constants.ENGINE, engineId);
//		prop.put(Constants.ENGINE_ALIAS, engineName);
//		prop.put(Constants.USERNAME, queryUtil.getUsername());
//		prop.put(Constants.PASSWORD, queryUtil.getPassword());
//		prop.put(Constants.DRIVER, queryUtil.getDriver());
//		prop.put(Constants.RDBMS_TYPE, queryUtil.getDbType().toString());
//		
//		// setting as a parameter for engine
//		//prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + dbName + System.getProperty("file.separator") + "insights_database");
//		prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + "@ENGINE@" + System.getProperty("file.separator") + "insights_database");
//		prop.put("TEMP", "TRUE");
//		prop.put("SCHEMA", schema);//schema comes from existing db (connect to external db(schema))
//		engine.open(prop);
//	}
//
////	//added for connect to external Impala workflow
////	private void connectToExternalImpalaEngine(String schema, String engineName, String engineId) throws Exception {
////		engine = new ImpalaEngine();
////		engine.setengineId(engineId);
////		engine.setEngineName(engineName);
////		Properties prop = new Properties();
//////		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
//////		prop.put(Constants.CONNECTION_URL, queryUtil.getConnectionURL(dbBaseFolder, SmssUtilities.getUniqueName(engineName, engineId)));
////		// grab from the query util directly
////		prop.put(Constants.CONNECTION_URL, this.queryUtil.getConnectionUrl());
////		prop.put(Constants.ENGINE, engineId);
////		prop.put(Constants.ENGINE_ALIAS, engineName);
////		prop.put(Constants.USERNAME, queryUtil.getUsername());
////		prop.put(Constants.PASSWORD, queryUtil.getPassword());
////		prop.put(Constants.DRIVER,queryUtil.getDriver());
////		prop.put(Constants.RDBMS_TYPE, queryUtil.getDbType().toString());
////
////		// setting as a parameter for engine
////		//prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + dbName + System.getProperty("file.separator") + "insights_database");
////		prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + "@ENGINE@" + System.getProperty("file.separator") + "insights_database");
////		prop.put("TEMP", "TRUE");
////		prop.put("SCHEMA", schema);//schema comes from existing db (connect to external db(schema))
////		((AbstractEngine) engine).setProp(prop);
////		engine.open(null);
////	}
//
//	protected void openEngineWithConnection(String engineId) {
//		engine = Utility.getDatabase(engineId);
//		owler = new Owler(engine);
//	}
//	
//	/**
//	 * Close the database engine
//	 * @throws IOException 
//	 */
//	protected void close() throws IOException {
//		logger.warn("Closing....");
//		if(engine != null) {
//			commitDB();
//			engine.close();
//		}
//	}	
//
//	protected void commitDB() throws IOException {
//		logger.warn("Committing....");
//		// commit the created engine
//		engine.commit();
//		
//		if(engine!=null && engine instanceof BigDataEngine){
//			((BigDataEngine)engine).infer();
//		} else if(engine!=null && engine instanceof RDFFileSesameEngine){
//			try {
//				((RDFFileSesameEngine)engine).exportDB();
//			} catch (Exception e) {
//				e.printStackTrace();
//				throw new IOException("Unable to commit data from file into database");
//			}
//		}
//	}
//
//	/**
//	 * Creates a repository connection to be put all the base relationship data
//	 * to create the OWL file
//	 * @throws Exception 
//	 * 
//	 * @throws EngineException
//	 */
//	protected void openOWLWithOutConnection(String engineId, String owlFile, IDatabaseEngine.DATABASE_TYPE dbType, String customBaseURI) throws Exception {
//		owler = new Owler(engineId, owlFile, dbType);
//		owler.addCustomBaseURI(customBaseURI);
//	}
//
//	/**
//	 * Close the OWL engine
//	 * @throws IOException 
//	 * @throws EngineException
//	 */
//	protected void closeOWL() throws IOException {
//		owler.closeOwl();
//	}
//	
//	/**
//	 * Writes the base information in the OWL to a file
//	 * @throws IOException 
//	 */
//	protected void createBaseRelations() throws IOException {
//		owler.commit();
//		try {
//			owler.export();
//		} catch (IOException ex) {
//			ex.printStackTrace();
//			throw new IOException("Unable to export OWL file...");
//		}
//	}
//	
//	/**
//	 * Fill in the sqlHash with the types
//	 */
//	protected void createSQLTypes() {
//		sqlHash.put("DECIMAL", "FLOAT");
//		sqlHash.put("DOUBLE", "FLOAT");
//		sqlHash.put("STRING", "VARCHAR(2000)"); // 8000 was chosen because this is the max for SQL Server; needs more permanent fix
//		sqlHash.put("TEXT", "VARCHAR(2000)"); // 8000 was chosen because this is the max for SQL Server; needs more permanent fix
//		//TODO: the FE needs to differentiate between "dates with times" vs. "dates"
//		sqlHash.put("DATE", "DATE");
//		sqlHash.put("SIMPLEDATE", "DATE");
//		// currently only add in numbers as doubles
//		sqlHash.put("NUMBER", "FLOAT");
//		sqlHash.put("INTEGER", "FLOAT");
//		sqlHash.put("BOOLEAN", "BOOLEAN");
//		
//		// TODO: standardized set of values
//		sqlHash.put(SemossDataType.BOOLEAN.toString(), "BOOLEAN");
//		sqlHash.put(SemossDataType.INT.toString(), "INT");
//		sqlHash.put(SemossDataType.DOUBLE.toString(), "FLOAT");
//		sqlHash.put(SemossDataType.STRING.toString(), "VARCHAR(2000)");
//		sqlHash.put(SemossDataType.DATE.toString(), "DATE");
//		sqlHash.put(SemossDataType.TIMESTAMP.toString(), "TIMESTAMP");
//
//	}
//}
