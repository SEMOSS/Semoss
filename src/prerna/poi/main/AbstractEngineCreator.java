package prerna.poi.main;

import java.io.IOException;
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
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
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
		prop.put(Constants.TEMP_CONNECTION_URL, queryUtil.getTempConnectionURL());
		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());
		prop.put(Constants.DREAMER, "db" + System.getProperty("file.separator") + dbName + System.getProperty("file.separator") + dbName + "_Questions.properties");
		prop.put("TEMP", "TRUE");
		((AbstractEngine) engine).setProperties(prop);
		engine.openDB(null);
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
	}
	
	private void createNewTinkerEngine(String dbName) {
		engine = new TinkerEngine();
		engine.setEngineName(dbName);
		engine.openDB(dbPropFile);
	}
	
	//added for connect to external RDBMS workflow
	protected void openRdbmsEngineWithConnection(String schema, String dbName) {
		connectToExternalRDBMSEngine(schema,dbName);
		openOWLWithOutConnection(owlFile, IEngine.ENGINE_TYPE.RDBMS, this.customBaseURI);
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
		prop.put(Constants.DRIVER,queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.TEMP_CONNECTION_URL, queryUtil.getTempConnectionURL());
		prop.put(Constants.RDBMS_TYPE,queryUtil.getDatabaseType().toString());
		prop.put(Constants.DREAMER, "db" + System.getProperty("file.separator") + dbName + System.getProperty("file.separator") + dbName + "_Questions.properties");
		prop.put("TEMP", "TRUE");
		prop.put("SCHEMA", schema);//schema comes from existing db (connect to external db(schema))
		((AbstractEngine) engine).setProperties(prop);
		engine.openDB(null);
	}

	protected void openEngineWithConnection(String engineName) {
		engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
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
		engine.commit();
		
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
		sqlHash.put("STRING", "VARCHAR(18000)"); // 8000 was chosen because this is the max for SQL Server; needs more permanent fix
		sqlHash.put("TEXT", "VARCHAR(18000)"); // 8000 was chosen because this is the max for SQL Server; needs more permanent fix
		//TODO: the FE needs to differentiate between "dates with times" vs. "dates"
		sqlHash.put("DATE", "DATE");
		sqlHash.put("SIMPLEDATE", "DATE");
		// currently only add in numbers as doubles
		sqlHash.put("NUMBER", "FLOAT");
		sqlHash.put("INTEGER", "FLOAT");
		sqlHash.put("BOOLEAN", "BOOLEAN");
	}
}
