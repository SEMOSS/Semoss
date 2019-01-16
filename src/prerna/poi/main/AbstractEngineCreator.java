package prerna.poi.main;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.r.RNativeEngine;
import prerna.engine.impl.rdbms.ImpalaEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;
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
	
	protected void openRdfEngineWithoutConnection(String appName, String appID) {
		createNewRdfEngine(appName, appID);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.SESAME, this.customBaseURI);
	}
	
	protected void openRdbmsEngineWithoutConnection(String appName, String engineID) {
		createNewRDBMSEngine(appName, engineID);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.RDBMS, this.customBaseURI);
	}
	
	protected void openTinkerEngineWithoutConnection(String appName, String appID) {
		createNewTinkerEngine(appName, appID);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.SESAME, this.customBaseURI);
	}
	
	protected void openREngineWithoutConnection(String appName, String appID) {
		createNewREngine(appName, appID);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.SESAME, this.customBaseURI);
	}
	
	private void createNewRDBMSEngine(String appName, String appID) {
		engine = new RDBMSNativeEngine();
		engine.setEngineId(appID);
		engine.setEngineName(appName);
		Properties prop = new Properties();
		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
		prop.put(Constants.CONNECTION_URL, queryUtil.getConnectionURL(dbBaseFolder,SmssUtilities.getUniqueName(appName, appID)));
		prop.put(Constants.ENGINE, appID);
		prop.put(Constants.ENGINE_ALIAS, appName);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER, queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.RDBMS_TYPE, queryUtil.getDatabaseType().toString());
		prop.put("TEMP", "TRUE");
		engine.setProp(prop);
		engine.openDB(null);
		
		// create the insight database
		if(engine.getInsightDatabase() == null) {
			IEngine insightDatabase = createNewInsightsDatabase(appName, appID);
			engine.setInsightDatabase(insightDatabase);
		}
	}

	private void createNewRdfEngine(String appName, String appID) {
		engine = new BigDataEngine();
		engine.setEngineId(appID);
		engine.setEngineName(appName);
		engine.openDB(dbPropFile);
		
		String sub = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
		String typeOf = RDF.TYPE.stringValue();
		String obj = Constants.CLASS_URI;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, typeOf, obj, true});
		
		sub =  semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
		obj = Constants.DEFAULT_PROPERTY_URI;
		engine.doAction(IEngine.ACTION_TYPE.ADD_STATEMENT, new Object[]{sub, typeOf, obj, true});
		
		// create the insight database
		if(engine.getInsightDatabase() == null) {
			IEngine insightDatabase = createNewInsightsDatabase(appName, appID);
			engine.setInsightDatabase(insightDatabase);
		}
	}
	
	private void createNewTinkerEngine(String appName, String appID) {
		engine = new TinkerEngine();
		engine.setEngineId(appID);
		engine.setEngineName(appName);
		engine.openDB(dbPropFile);
		
		// create the insight database
		if(engine.getInsightDatabase() == null) {
			IEngine insightDatabase = createNewInsightsDatabase(appName, appID);
			engine.setInsightDatabase(insightDatabase);
		}
	}
	
	private void createNewREngine(String appName, String appID) {
		engine = new RNativeEngine();
		engine.setEngineName(appName);
		engine.setEngineId(appID);
		engine.openDB(dbPropFile);
		
		// create the insight database
		if(engine.getInsightDatabase() == null) {
			IEngine insightDatabase = createNewInsightsDatabase(appName, appID);
			engine.setInsightDatabase(insightDatabase);
		}
	}
	
	//added for connect to external RDBMS workflow
	protected void generateEngineFromRDBMSConnection(String schema, String appName, String appID) {
		connectToExternalRDBMSEngine(schema,appName, appID);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.RDBMS, this.customBaseURI);
	}
	
	//added for connect to external Impala workflow
	protected void generateEngineFromImpalaConnection(String schema, String dbName, String appID) {
		connectToExternalImpalaEngine(schema,dbName, appID);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.IMPALA, this.customBaseURI);
	}
	//added for connect to external RDBMS workflow
	private void connectToExternalRDBMSEngine(String schema, String appName, String appID) {
		engine = new RDBMSNativeEngine();
		engine.setEngineId(appID);
		engine.setEngineName(appName);
		Properties prop = new Properties();
		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
		prop.put(Constants.CONNECTION_URL, queryUtil.getConnectionURL(dbBaseFolder, SmssUtilities.getUniqueName(appName, appID)));
		prop.put(Constants.ENGINE, appID);
		prop.put(Constants.ENGINE_ALIAS, appName);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER, queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.RDBMS_TYPE, queryUtil.getDatabaseType().toString());
		
		// setting as a parameter for engine
		//prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + dbName + System.getProperty("file.separator") + "insights_database");
		prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + "@ENGINE@" + System.getProperty("file.separator") + "insights_database");
		prop.put("TEMP", "TRUE");
		prop.put("SCHEMA", schema);//schema comes from existing db (connect to external db(schema))
		((AbstractEngine) engine).setProp(prop);
		engine.openDB(null);
		
		// create the insight database
		if(engine.getInsightDatabase() == null) {
			IEngine insightDatabase = createNewInsightsDatabase(appName, appID);
			engine.setInsightDatabase(insightDatabase);
		}
	}

	//added for connect to external Impala workflow
	private void connectToExternalImpalaEngine(String schema, String appName, String appID) {
		engine = new ImpalaEngine();
		engine.setEngineId(appID);
		engine.setEngineName(appName);
		Properties prop = new Properties();
		String dbBaseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", System.getProperty("file.separator"));
		prop.put(Constants.CONNECTION_URL, queryUtil.getConnectionURL(dbBaseFolder, SmssUtilities.getUniqueName(appName, appID)));
		prop.put(Constants.ENGINE, appID);
		prop.put(Constants.ENGINE_ALIAS, appName);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER,queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());

		// setting as a parameter for engine
		//prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + dbName + System.getProperty("file.separator") + "insights_database");
		prop.put(Constants.RDBMS_INSIGHTS, "db" + System.getProperty("file.separator") + "@ENGINE@" + System.getProperty("file.separator") + "insights_database");
		prop.put("TEMP", "TRUE");
		prop.put("SCHEMA", schema);//schema comes from existing db (connect to external db(schema))
		((AbstractEngine) engine).setProp(prop);
		engine.openDB(null);

		// create the insight database
		if(engine.getInsightDatabase() == null) {
			IEngine insightDatabase = createNewInsightsDatabase(appName, appID);
			engine.setInsightDatabase(insightDatabase);
		}
	}

	protected void openEngineWithConnection(String engineId) {
		engine = Utility.getEngine(engineId);
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
			} catch (Exception e) {
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
		
		// TODO: standardized set of values
		sqlHash.put(SemossDataType.BOOLEAN.toString(), "BOOLEAN");
		sqlHash.put(SemossDataType.INT.toString(), "INT");
		sqlHash.put(SemossDataType.DOUBLE.toString(), "FLOAT");
		sqlHash.put(SemossDataType.STRING.toString(), "VARCHAR(2000)");
		sqlHash.put(SemossDataType.DATE.toString(), "DATE");
		sqlHash.put(SemossDataType.TIMESTAMP.toString(), "TIMESTAMP");

	}
	
	protected IEngine createNewInsightsDatabase(String appName, String appId) {
		IEngine insightEngine = UploadUtilities.generateInsightsDatabase(appId, appName);
		UploadUtilities.addExploreInstanceInsight(appId, insightEngine);
		return insightEngine;
	}
}
