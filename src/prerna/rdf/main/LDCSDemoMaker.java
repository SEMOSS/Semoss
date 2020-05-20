//package prerna.rdf.main;
//
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.sql.Statement;
//
//import prerna.algorithm.api.SemossDataType;
//import prerna.date.SemossDate;
//import prerna.ds.rdbms.postgres.PostgresFrame;
//import prerna.poi.main.helper.CSVFileHelper;
//import prerna.test.TestUtilityMethods;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.sql.AbstractSqlQueryUtil;
//import prerna.util.sql.RdbmsTypeEnum;
//import prerna.util.sql.SqlQueryUtilFactor;
//import java.util.Properties;
//
//import org.apache.log4j.Logger;
//
//public class LDCSDemoMaker {
//
//	private static Logger logger = Logger.getLogger(LDCSDemoMaker.class);
//
//	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
//
//
//	private static final String CONFIGURATION_FILE = "config.properties";
//	private static final String MYSQL_PASSWORD = "mysql_password";
//	private static final String POSTGRES_PASSWORD = "postgres_password";
//
//	private static final String VARCHAR_100 = "VARCHAR(100)";
//	private static final String ENC_ID = "ENC_ID";
//	private static final String ENCOUNTER = "ENCOUNTER";
//	private static final String FLOAT = "FLOAT";
//	static boolean useH2 = false;
//
//	private static final String CLAIMS = "claims";
//	static String[] claimsHeaders = new String[] {"BILLABLE_PERIOD", "DIAGNOSIS", ENC_ID, "ID", "ORGANIZATION", "SOCIAL_SECURITY_NUM", "TOTAL"};
//	static String[] claimsTypes = new String[] {"DATE", VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, FLOAT};
//	
//	// ENCOUNTERS IS SLIGHTLY DIFFERENT
//	// IT HAS THE THE PROC_DATE AS WELL
//	private static final String ENCOUNTERS = "encounters";
//	static String[] encountersHeaders = new String[] {"ENC_CD", "ENC_DESC", "ENC_DT", "ENC_REASON_CD", "ENC_REASON_DESC", ENC_ID, "SSN", "FACILITY" };
//	static String[] encountersTypes = new String[] {VARCHAR_100, VARCHAR_100, VARCHAR_100, FLOAT, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100};
//	
//	static String[] encountersFullHeaders = new String[] {"ENC_CD", "ENC_DESC", "ENC_DT", "ENC_REASON_CD", "ENC_REASON_DESC", ENC_ID, "SSN", "FACILITY", "PROC_DATE" };
//	static String[] encountersFullTypes = new String[] {VARCHAR_100, VARCHAR_100, "DATE", FLOAT, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, "DATE"};
//	
//	/*
//	 *  all the pampi files
//	 */
//	
//	static String[] allgergiesHeaders = new String[] {"ALL_CD", "ALL_DESC", "ALL_START", "ALL_STOP", "ENCNTR", "SSN"};
//	static String[] allergiesTypes = new String[] {FLOAT, VARCHAR_100, "DATE", "DATE", VARCHAR_100, VARCHAR_100};
//	
//	static String[] careplansHeaders = new String[] {"CAR_CD", "CAR_DESC", "CAR_REASON_CD", "CAR_REASON_DESC", "CAR_START", "CAR_STOP", ENCOUNTER, "ID", "SOCIAL_SECURITY_NUMBER"};
//	static String[] careplansTypes = new String[] {FLOAT, VARCHAR_100, FLOAT, VARCHAR_100, "DATE", "DATE", VARCHAR_100, VARCHAR_100, VARCHAR_100};
//	
//	static String[] conditionsHeaders = new String[] {"CON_CD", "CON_DESC", "CON_START", "CON_STOP", ENCOUNTER, "US_SOCIAL"};
//	static String[] conditionsTypes = new String[] {FLOAT, VARCHAR_100, "DATE", "DATE", VARCHAR_100, VARCHAR_100};
//
//	static String[] immunizationsHeaders = new String[] {ENCOUNTER, "IMM_CD", "IMM_DESC", "IMM_DT", "US_IDENTIFICATION_NUMBER"};
//	static String[] immunizationsTypes = new String[] {VARCHAR_100, FLOAT, VARCHAR_100, "DATE", VARCHAR_100};
//	
//	static String[] medicationsHeaders = new String[] {"ENC", "MED_CD", "MED_DESC", "MED_REASON_DESC", "MED_START", "MED_STOP", "REASONDESCRIPTION", "SOCIAL_SECURITY"};
//	static String[] medicationsTypes = new String[] {VARCHAR_100, FLOAT, VARCHAR_100, VARCHAR_100, "DATE", "DATE", VARCHAR_100, VARCHAR_100};
//
//	static String[] observationsHeaders = new String[] {"ENC", "OBS_CD", "OBS_DESC", "OBS_DT", "SS_NUMBER", "UNITS", "VAL"};
//	static String[] observationsTypes = new String[] {VARCHAR_100, VARCHAR_100, VARCHAR_100, "DATE", VARCHAR_100, VARCHAR_100, VARCHAR_100};
//
//	private static final String PATIENTS = "patients";
//	private static final String PATIENTS_CSV = "/patients.csv";
//	static String[] patientsHeaders = new String[] {"ADDRESS", "BIRTHDATE", "BIRTHPLACE", "DEATHDATE", "DRIVERS", "ETHNICITY", "FIRST", "GENDER", "LAST", "MAIDEN", "MARITAL", "PASSPORT", "PREFIX", "RACE", "SOCIAL_SECURITY_NUMBER", "SUFFIX"};
//	static String[] patientsTypes = new String[] {VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100, VARCHAR_100};
//
//	public static void main(String[] args) throws Exception {
//		String basePath = "C:\\workspace\\Semoss_Dev";
//		TestUtilityMethods.loadAll(basePath + "\\RDF_Map.prop");
//		
//		long start = System.currentTimeMillis();
//		
//		// load the database
//		Connection cabacusMtf = null;
//		Connection cabacusMaster = null;
//		Connection pampi = null;
//		
//		AbstractSqlQueryUtil postgresHelper = SqlQueryUtilFactor.initialize(RdbmsTypeEnum.POSTGRES);
//		AbstractSqlQueryUtil mySqlHelper = SqlQueryUtilFactor.initialize(RdbmsTypeEnum.MYSQL);
//
//		if(useH2) {
//			// NOTE ::: THESE USE THE basePath VARIABLE TO WRITE THE FILE!!!
//			// THIS WILL MAKE A master.database, mtf.database, and pampi.database in that folder
//			// and you can drag n drop the h2 file in the upload UI
//			cabacusMtf = getH2CabacusMTF(basePath);
//			cabacusMaster = getH2CabacusMaster(basePath);
//			pampi = getH2Nemesis(basePath);
//			
//			// reassign to H2 anyway... dont worry about name
//			postgresHelper = SqlQueryUtilFactor.initialize(RdbmsTypeEnum.H2_DB);
//			mySqlHelper = SqlQueryUtilFactor.initialize(RdbmsTypeEnum.H2_DB);
//		} else {
//			cabacusMtf = getCabacusMTF();
//			cabacusMaster = getCabacusMaster();
//			pampi = getNemesis();
//		}
//
//		if (cabacusMtf != null) {
//			cabacusMtf.setAutoCommit(false);
//		}
//		
//		if (cabacusMaster != null) {
//			cabacusMaster.setAutoCommit(false);
//		}
//		
//		if (pampi !=null) {
//			pampi.setAutoCommit(false);
//		}
//
//		String directory = "C:\\Users\\mahkhalil\\Desktop\\PAMPI";
//
//		// first, CABACUS_MTF
//		// need to load 2 different files
//		{
//			// claims
//			{
//				String file = directory + "/subset_claims.csv";
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(cabacusMtf, postgresHelper, helper, CLAIMS, claimsHeaders, claimsTypes);
//			}
//			// encounters
//			{
//				String file = directory + "/subset_encounters.csv";
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(cabacusMtf, postgresHelper, helper, ENCOUNTERS, encountersHeaders, encountersTypes);
//			}
//			// patients
//			{
//				String file = directory + PATIENTS_CSV;
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(cabacusMtf, postgresHelper, helper, PATIENTS, patientsHeaders, patientsTypes);
//			}
//		}
//		
//		// second, CABACUS_Master
//		// need to load 2 different files
//		{
//			// claims
//			{
//				String file = directory + "/claims.csv";
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(cabacusMaster, mySqlHelper, helper, CLAIMS, claimsHeaders, claimsTypes);
//			}
//			// also load the other claims information as well
//			{
//				String file = directory + "/subset_claims.csv";
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(cabacusMaster, mySqlHelper, helper, CLAIMS, claimsHeaders, claimsTypes, false);
//			}
//			// encounters
//			{
//				String file = directory + "/encounters.csv";
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(cabacusMaster, mySqlHelper, helper, ENCOUNTERS, encountersFullHeaders, encountersFullTypes);
//			}
//			// patients
//			{
//				String file = directory + PATIENTS_CSV;
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(cabacusMaster, mySqlHelper, helper, PATIENTS, patientsHeaders, patientsTypes);
//			}
//		}
//		
//		// PAMPI
//		{
//			// allergies
//			{
//				String file = directory + "/allergies.csv";
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(pampi, mySqlHelper, helper, "allergies", allgergiesHeaders, allergiesTypes);
//			}
//			// careplans
//			{
//				String file = directory + "/careplans.csv";
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(pampi, mySqlHelper, helper, "careplans", careplansHeaders, careplansTypes);
//			}
//			// conditions
//			{
//				String file = directory + "/conditions.csv";
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(pampi, mySqlHelper, helper, "conditions", conditionsHeaders, conditionsTypes);
//			}
//			// immunizations
//			{
//				String file = directory + "/immunizations.csv";
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(pampi, mySqlHelper, helper, "immunizations", immunizationsHeaders, immunizationsTypes);
//			}
//			// medications
//			{
//				String file = directory + "/medications.csv";
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(pampi, mySqlHelper, helper, "medications", medicationsHeaders, medicationsTypes);
//			}
//			// observations
//			{
//				String file = directory + "/observations.csv";
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(pampi, mySqlHelper, helper, "observations", observationsHeaders, observationsTypes);
//			}
//			// patients
//			{
//				String file = directory + PATIENTS_CSV;
//				CSVFileHelper helper = new CSVFileHelper();
//				helper.parse(file);
//				loadCsvFile(pampi, mySqlHelper, helper, PATIENTS, patientsHeaders, patientsTypes);
//			}
//		}
//
//		if (cabacusMtf != null) {
//			cabacusMtf.commit();
//		}
//
//		if (cabacusMaster != null) {
//			cabacusMaster.commit();
//		}
//
//		if (pampi !=null) {
//			pampi.commit();
//		}
//
//		long end = System.currentTimeMillis();
//		logger.debug("Time to finish = " + (end - start) + "ms");
//	}
//
//	private static void loadCsvFile(Connection conn, AbstractSqlQueryUtil queryUtil, CSVFileHelper helper, String tableName, String[] columnNames, String[] types) throws Exception {
//		loadCsvFile(conn, queryUtil, helper, tableName, columnNames, types, true);
//	}
//	
//	private static void loadCsvFile(Connection conn, AbstractSqlQueryUtil queryUtil, CSVFileHelper helper, String tableName, String[] columnNames, String[] types, boolean remake) throws Exception {
//		if(remake) {
//			// drop table if exists
//			{
//				String createTable = queryUtil.dropTableIfExists(tableName);
//				Statement stmt = conn.createStatement();
//				stmt.execute(createTable);
//				stmt.close();
//			}
//			
//			// create table if not exists
//			{
//				String createTable = queryUtil.createTableIfNotExists(tableName, columnNames, types);
//				Statement stmt = conn.createStatement();
//				stmt.execute(createTable);
//				stmt.close();
//			}
//		}
//		
//		String bulkInsert = queryUtil.createInsertPreparedStatementString(tableName, columnNames);
//		PreparedStatement ps = conn.prepareStatement(bulkInsert);
//
//		SemossDataType[] sTypes = new SemossDataType[types.length];
//		for(int i = 0; i < types.length; i++) {
//			sTypes[i] = SemossDataType.convertStringToDataType(types[i]);
//		}
//		
//		int counter = 0;
//		String[] row = null;
//		while( (row = helper.getNextRow()) != null ) {
//			for(int i = 0; i < row.length; i++) {
//				String value = row[i];
//				
//				if(sTypes[i] == SemossDataType.STRING) {
//					if(value.equals("NaN")) {
//						ps.setNull(i+1, java.sql.Types.VARCHAR);
//					} else {
//						ps.setString(i+1, value);
//					}
//					
//					
//				} else if(sTypes[i] == SemossDataType.DOUBLE) {
//					if(value.isEmpty() || value.equals("NaN")) {
//						ps.setNull(i+1, java.sql.Types.DOUBLE);
//					} else {
//						Double d = Double.parseDouble(value);
//						ps.setDouble(i+1, d);
//					}
//					
//					
//				} else if(sTypes[i] == SemossDataType.INT) {
//					if(value.isEmpty() || value.equals("NaN")) {
//						ps.setNull(i+1, java.sql.Types.INTEGER);
//					} else {
//						Integer d = Integer.parseInt(value);
//						ps.setInt(i+1, d);
//					}
//					
//					
//				} else if(sTypes[i] == SemossDataType.DATE) {
//					if(value.isEmpty() || value.equals("NaN")) {
//						ps.setNull(i+1, java.sql.Types.DATE);
//					} else {
//						SemossDate date = SemossDate.genDateObj(value);
//						ps.setDate(i+1, java.sql.Date.valueOf(date.getFormatted("yyyy-MM-dd")));
//					}
//					
//				}
//			}
//			
//			ps.addBatch();
//			if(++counter % 1000 == 0) {
//				logger.debug(tableName + " Executing batch row num = " + counter);
//				ps.executeBatch();
//			}
//		}
//		
//		// execute whatever left over
//		ps.executeBatch();
//		logger.debug("Done with " + tableName + " total rows = " + counter);
//	}
//
//	/**
//	 * THIS WILL RETURN THE POSTGRES CABACUS_MTF SCHEMA
//	 * @return
//	 */
//	private static Connection getCabacusMTF() throws Exception {
//		Class.forName(RdbmsTypeEnum.POSTGRES.getDriver());
//		String connectionUrl = RdbmsTypeEnum.POSTGRES.getUrlPrefix() + "://localhost:5432/postgres?currentSchema=mtf";
//		Connection conn = null;
//
//		try(InputStream input = new FileInputStream(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + CONFIGURATION_FILE)) {
//			Properties prop = new Properties();
//			prop.load(input);
//			conn = DriverManager.getConnection(connectionUrl, "postgres", prop.getProperty(POSTGRES_PASSWORD));
//		} catch (IOException ex) {
//			logger.error("Error with loading properties in config file for Postgres CABACUS_MTF Schema" + ex.getMessage());
//		}
//
//		return conn;
//	}
//
//	/**
//	 * THIS WILL RETURN THE MYSQL CABACUS_MASTER SCHEMA
//	 * @return
//	 */
//	private static Connection getCabacusMaster() throws Exception {
//		Class.forName(RdbmsTypeEnum.MYSQL.getDriver());
//		String connectionUrl = RdbmsTypeEnum.MYSQL.getUrlPrefix() + "://localhost:3306/master";
//		Connection conn = null;
//
//		try(InputStream input = new FileInputStream(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + CONFIGURATION_FILE)) {
//			Properties prop = new Properties();
//			prop.load(input);
//			conn = DriverManager.getConnection(connectionUrl, "root", prop.getProperty(MYSQL_PASSWORD));
//		} catch (IOException ex) {
//			logger.error("Error with loading properties in config file for MySQL CABACUS_MASTER Schema" + ex.getMessage());
//		}
//
//		return conn;
//	}
//
//	/**
//	 * THIS WILL RETURN THE MYSQL PAMPI
//	 * @return
//	 */
//	private static Connection getNemesis() throws Exception {
//		Class.forName(RdbmsTypeEnum.MYSQL.getDriver());
//		String connectionUrl = RdbmsTypeEnum.MYSQL.getUrlPrefix() + "://localhost:3306/pampi";
//		Connection conn = null;
//
//		try(InputStream input = new FileInputStream(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + CONFIGURATION_FILE)) {
//			Properties prop = new Properties();
//			prop.load(input);
//			conn = DriverManager.getConnection(connectionUrl, "root", prop.getProperty(MYSQL_PASSWORD));
//		} catch (IOException ex) {
//			logger.error("Error with loading properties in config file for MySQL PAMPI" + ex.getMessage());
//		}
//
//		return conn;
//	}
//
//	/**
//	 * THIS WILL RETURN THE H2 CABACUS_MTF SCHEMA
//	 * @return
//	 */
//	private static Connection getH2CabacusMTF(String basePath) throws Exception {
//		Class.forName(RdbmsTypeEnum.H2_DB.getDriver());
//		String connectionUrl = RdbmsTypeEnum.H2_DB.getUrlPrefix() +  ":nio:" + basePath + "/mtf";
//		Connection conn = DriverManager.getConnection(connectionUrl, "", "");
//		return conn;
//	}
//
//	/**
//	 * THIS WILL RETURN TEHE H2 CABACUS_MASTER SCHEMA
//	 * @return
//	 */
//	private static Connection getH2CabacusMaster(String basePath) throws Exception {
//		Class.forName(RdbmsTypeEnum.H2_DB.getDriver());
//		String connectionUrl = RdbmsTypeEnum.H2_DB.getUrlPrefix() +  ":nio:" + basePath + "/master";
//		Connection conn = DriverManager.getConnection(connectionUrl, "", "");
//		return conn;
//	}
//
//	/**
//	 * THIS WILL RETURN THE H2 PAMPI
//	 * @return
//	 */
//	private static Connection getH2Nemesis(String basePath) throws Exception {
//		Class.forName(RdbmsTypeEnum.H2_DB.getDriver());
//		String connectionUrl = RdbmsTypeEnum.H2_DB.getUrlPrefix() +  ":nio:" + basePath + "/pampi";
//		Connection conn = DriverManager.getConnection(connectionUrl, "", "");
//		return conn;
//	}
//	
//	
//}
