package prerna.reactor.algorithms.xray;
//package prerna.sablecc2.reactor.algorithms.xray;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.io.IOException;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.Vector;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.commons.io.FilenameUtils;
//import org.apache.logging.log4j.Logger;
//
//import au.com.bytecode.opencsv.CSVReader;
//import prerna.ds.r.RSyntaxHelper;
//import prerna.ds.util.RdbmsQueryBuilder;
//import prerna.engine.api.IEngine;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
//import prerna.nameserver.utility.MasterDatabaseUtility;
//import prerna.poi.main.helper.XLFileHelper;
//import prerna.query.querystruct.AbstractQueryStruct;
//import prerna.query.querystruct.SelectQueryStruct;
//import prerna.query.querystruct.selectors.QueryColumnSelector;
//import prerna.query.querystruct.selectors.QueryFunctionHelper;
//import prerna.query.querystruct.selectors.QueryFunctionSelector;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
//import prerna.util.Constants;
//import prerna.util.Utility;
//
//public class Xray {
//
//	private static final String STACKTRACE = "StackTrace: ";
//	public static final String ENGINE_CONCEPT_PROPERTY_DELIMETER = ";";
//	private AbstractRJavaTranslator rJavaTranslator = null;
//	private String baseFolder = null;
//	private Logger logger = null;
//
//	// variables used to create instance count frame
//	private boolean genCountFrame = false;
//	private String countDF = null;
//	private List<String> engineColumn = new Vector<>();
//	private List<String> tableColumn = new Vector<>();
//	private List<String> propColumn = new Vector<>();
//	private List<Integer> countColumn = new Vector<>();
//	// get list of engines used
//	private Set<String> engineList = new HashSet<>();
//
//	public Xray(AbstractRJavaTranslator rJavaTranslator, String baseFolder, Logger logger) {
//		this.rJavaTranslator = rJavaTranslator;
//		this.baseFolder = baseFolder;
//		this.logger = logger;
//	}
//
//	public String run(Map config) {
//		logger.info("Checking if required R packages are installed to run X-ray...");
//		// packages to compare corpus
//		String[] packages = new String[] { "textreuse", "RcppProgress", "withr", "NLP", "tidyr", "devtools", "memoise", "digest", "tidyselect", "purrr"};
//		this.rJavaTranslator.checkPackages(packages);
//		// packages to write corpus files
//		String[] encodePackages = new String[] {"WikidataR", "WikipediR", "httr", "curl", "jsonlite"};
//		this.rJavaTranslator.checkPackages(encodePackages);
//
//
//
//		// output folder for data mode to be written to
//		String dataFolder = this.baseFolder + "\\R\\XrayCompatibility\\Temp\\MatchingRepository";
//		dataFolder = dataFolder.replace("\\", "/");
//		// output folder for semantic data to be written to
//		String semanticFolder = this.baseFolder + "\\R\\XrayCompatibility\\Temp\\SemanticRepository";
//		semanticFolder = semanticFolder.replace("\\", "/");
//		// clean output folders
//		try {
//			FileUtils.cleanDirectory(new File(dataFolder));
//			FileUtils.cleanDirectory(new File(semanticFolder));
//		} catch (IOException e1) {
//			logger.error(STACKTRACE, e1);
//		}
//
//		// get xray parameters
//		Map<String, Object> parameters = (Map<String, Object>) config.get("parameters");
//		boolean semanticMode = getSemanticMode(parameters);
//		boolean dataMode = getDataMode(parameters);
//		// if no mode is defined set xray to data mode
//		if ((!semanticMode && !dataMode)) {
//			dataMode = true;
//		}
//
//		// Write text files to run xray comparison from different sources
//		// outputFolder/database;table;column.txt
//		HashMap<String, String> engineNameLookup = new HashMap<>();
//		List<Object> connectors = (List<Object>) config.get("connectors");
//		for (int i = 0; i < connectors.size(); i++) {
//			Map<String, Object> connection = (Map<String, Object>) connectors.get(i);
//			String connectorType = (String) connection.get("connectorType");
//			Map<String, Object> connectorData = (Map<String, Object>) connection.get("connectorData");
//			Map<String, Object> dataSelection = (Map<String, Object>) connection.get("dataSelection");
//			if (connectorType.equalsIgnoreCase("LOCAL")) {
//				writeLocalEngineToFile(connectorData, dataSelection, dataMode, dataFolder, semanticMode,
//						semanticFolder);
//				String id = connectorData.get("engineId") + "";
//				engineNameLookup.put(id, MasterDatabaseUtility.getDatabaseAliasForId(id));
//			} else if (connectorType.equalsIgnoreCase("EXTERNAL")) {
//				writeExternalToFile(connectorData, dataSelection, dataMode, dataFolder, semanticMode, semanticFolder);
//			} else if (connectorType.equalsIgnoreCase("FILE")) {
//				// process csv file reading
//				String sourceFile = (String) connectorData.get("filePath");
//				String extension = FilenameUtils.getExtension(sourceFile);
//				if (extension.equals("csv") || extension.equals("txt")) {
//					writeCsvToFile(sourceFile, dataSelection, dataMode, dataFolder, semanticMode, semanticFolder);
//				} else if (extension.equals("xls") || extension.equals("xlsx")) {
//					writeExcelToFile(sourceFile, dataSelection, connectorData, dataMode, dataFolder, semanticMode,
//							semanticFolder);
//				}
//			}
//		}
//		// build instance count data frame to join results
//		if(this.genCountFrame) { 
//			if (!this.engineColumn.isEmpty()) {
//				this.countDF = "countDF" + Utility.getRandomString(8);
//				StringBuilder countBuilder = new StringBuilder();
//				countBuilder
//				.append(this.countDF + "<- data.frame(engine="
//						+ RSyntaxHelper.createStringRColVec(engineColumn.toArray()) + ", table="
//						+ RSyntaxHelper.createStringRColVec(tableColumn.toArray()) + ", prop="
//						+ RSyntaxHelper.createStringRColVec(propColumn.toArray()) + ", count=" + RSyntaxHelper
//						.createStringRColVec(countColumn.toArray(new Integer[countColumn.size()]))
//						+ ")");
//				this.rJavaTranslator.runR(countBuilder.toString());
//			}
//		}
//
//		// get other parameters for xray script
//		int nMinhash = 0;
//		int nBands = 0;
//		int instancesThreshold = 1;
//		double similarityThreshold = getSimiliarityThreshold(parameters);
//		double candidateThreshold = getCandidateThreshold(parameters);
//		// check if user wants to compare columns from the same database
//		// this is the boolean value passed into R script
//		Boolean matchSameDB = getMatchSameDB(parameters);
//		if (candidateThreshold <= 0.03) {
//			nMinhash = 3640;
//			nBands = 1820;
//		} else if (candidateThreshold <= 0.02) {
//			nMinhash = 8620;
//			nBands = 4310;
//		} else if (candidateThreshold <= 0.01) {
//			nMinhash = 34480;
//			nBands = 17240;
//		} else if (candidateThreshold <= 0.05) {
//			nMinhash = 1340;
//			nBands = 670;
//		} else if (candidateThreshold <= 0.1) {
//			nMinhash = 400;
//			nBands = 200;
//		} else if (candidateThreshold <= 0.2) {
//			nMinhash = 200;
//			nBands = 100;
//		} else if (candidateThreshold <= 0.4) {
//			nMinhash = 210;
//			nBands = 70;
//		} else if (candidateThreshold <= 0.5) {
//			nMinhash = 200;
//			nBands = 50;
//		} else {
//			nMinhash = 200;
//			nBands = 40;
//		}
//
//		String baseMatchingFolder = this.baseFolder + "\\R\\XrayCompatibility";
//		baseMatchingFolder = baseMatchingFolder.replace("\\", "/");
//		// print xray data mode results to csv
//		// Semoss/R/Matching/Temp/rdbms
//		String outputXrayDataFolder = baseMatchingFolder + "\\Temp\\rdbms";
//		outputXrayDataFolder = outputXrayDataFolder.replace("\\", "/");
//
//		// Parameters for R script
//		String rFrameName = "xray" + Utility.getRandomString(8);
//
//		// Grab the utility script
//		String utilityScriptPath = baseMatchingFolder + "\\" + "matching.R";
//		utilityScriptPath = utilityScriptPath.replace("\\", "/");
//
//		// Create R Script to run xray
//		StringBuilder rsb = new StringBuilder();
//		rsb.append(RSyntaxHelper.loadPackages(packages));
//
//		// Source the LSH function from the utility script
//		rsb.append("source(\"" + utilityScriptPath + "\");");
//		rsb.append(rFrameName + " <- data.frame();");
//
//		// Run locality sensitive hashing to generate matches
//		String lookupFrame = "lookup"+ Utility.getRandomString(8);
//		// create a lookup table for engineId to engineAlias
//		rsb.append(lookupFrame + " <- data.frame(engineId = character(), engineName = character(), stringsAsFactors=FALSE);");
//		int row = 1;
//		for(String key: engineNameLookup.keySet()){
//			rsb.append(lookupFrame + "[" + row + ", ]<-c(\"" + key + "\",\"" + engineNameLookup.get(key) + "\");");
//			row++;
//		}
//		if (dataMode) {
//			rsb.append(rFrameName + " <- " + Constants.R_LSH_MATCHING_FUN + "(\"" + dataFolder + "\", " + nMinhash
//					+ ", " + nBands + ", " + similarityThreshold + ", " + instancesThreshold + ", \""
//					+ ENGINE_CONCEPT_PROPERTY_DELIMETER + "\", " + matchSameDB.toString().toUpperCase()
//					+ ", \"" + outputXrayDataFolder + "\");");
//
//			rsb.append(rFrameName + " <-merge(" + rFrameName + "," + lookupFrame + ", by.x=\"Source_Database_Id\", by.y=\"engineId\");colnames(" + rFrameName + ")[13] <- \"Source_Database\";");
//			rsb.append(rFrameName + " <-merge(" + rFrameName + "," + lookupFrame +", by.x=\"Target_Database_Id\", by.y=\"engineId\");colnames(" + rFrameName + ")[14] <- \"Target_Database\";");
//		}
//		this.logger.info("Comparing data from datasources for X-ray data mode...");
//		this.rJavaTranslator.runR(rsb.toString());
//		String semanticComparisonFrame = "semantic.xray.df";
//
//		// run xray on semantic folder
//		if (semanticMode) {
//			rsb = new StringBuilder();
//
//			// Source the LSH function from the utility script
//			rsb.append("source(\"" + utilityScriptPath + "\");");
//			rsb.append(semanticComparisonFrame + " <- data.frame();");
//			String semanticOutputFolder = baseMatchingFolder + "\\Temp\\semantic";
//			semanticOutputFolder = semanticOutputFolder.replace("\\", "/");
//			rsb.append(semanticComparisonFrame + " <- " + Constants.R_LSH_MATCHING_FUN + "(\"" + semanticFolder + "\", "
//					+ nMinhash + ", " + nBands + ", " + similarityThreshold + ", " + instancesThreshold + ", \""
//					+ ENGINE_CONCEPT_PROPERTY_DELIMETER + "\", " + matchSameDB.toString().toUpperCase()
//					+ ", \"" + semanticOutputFolder + "\");");
//			rsb.append(semanticComparisonFrame + " <-merge(" + semanticComparisonFrame + "," + lookupFrame + ", by.x=\"Source_Database_Id\", by.y=\"engineId\");colnames(" + semanticComparisonFrame + ")[13] <- \"Source_Database\";");
//			rsb.append(semanticComparisonFrame + " <-merge(" + semanticComparisonFrame + "," + lookupFrame +", by.x=\"Target_Database_Id\", by.y=\"engineId\");colnames(" + semanticComparisonFrame + ")[14] <- \"Target_Database\";");
//			// join data xray df with semantic xray df if dataComparison frame
//			// was created
//			if (dataMode) {
//				String mergeRScriptPath = baseMatchingFolder + "\\merge.r";
//				mergeRScriptPath = mergeRScriptPath.replace("\\", "/");
//				rsb.append("source(\"" + mergeRScriptPath + "\");");
//				rsb.append(rFrameName + " <- xray_merge(" + rFrameName + ", " + semanticComparisonFrame + ");");
//			} else {
//				rsb.append(rFrameName + " <-" + semanticComparisonFrame + ";");
//				// if it is only semantic score renmae score header to semnatic score
//				rsb.append("names(" + rFrameName + ")[names(" + rFrameName + ") == 'Score'] <- 'Semantic_Score';");
//
//			}
//			this.logger.info("Comparing data from datasources for X-ray semantic mode...");
//			this.rJavaTranslator.runR(rsb.toString());
//
//		}
//
//		// clean up r variables for semantic comparison
//		StringBuilder cleanUpScript = new StringBuilder();
//		cleanUpScript.append("rm(" + semanticComparisonFrame + ");");
//		cleanUpScript.append("rm(" + "xray_merge" + ");");
//		cleanUpScript.append("rm(" + "concept_mgr" + ");");
//		cleanUpScript.append("rm(" + "concept_xray" + ");");
//		cleanUpScript.append("rm(" + "encode_instances" + ");");
//		cleanUpScript.append("rm(" + "get_claims" + ");");
//		cleanUpScript.append("rm(" + "get_concept" + ");");
//		cleanUpScript.append("rm(" + "get_wiki_ids" + ");");
//		cleanUpScript.append("rm(" + "is.letter" + ");");
//		cleanUpScript.append("rm(" + "most_frequent_concept" + ");");
//		cleanUpScript.append("rm(" + "run_lsh_matching" + ");");
//		cleanUpScript.append("rm(" + "span" + ");");
//		cleanUpScript.append("rm(" + lookupFrame + ");");
//		this.rJavaTranslator.runR(cleanUpScript.toString());
//		return rFrameName;
//	}
//
//	/**
//	 * Encrypts data from an excel file to files named sheetName;column.txt for
//	 * the selected columns
//	 * 
//	 * @param excelFile
//	 *            the location of the excelFile
//	 * @param dataSelection
//	 *            determines the specific columns from the excel file to write
//	 *            to text files from the xray config file
//	 * @param dataMode
//	 *            enables data mode compare instance data
//	 * @param dataFolder
//	 *            the location where encrypted data from data mode is written to
//	 * @param semanticMode
//	 *            enables semantic mode to compare wiki header data
//	 * @param semanticFolder
//	 *            the location where data from semantic mode is written to
//	 */
//	private void writeExcelToFile(String excelFile, Map<String, Object> dataSelection,
//			Map<String, Object> connectorData, boolean dataComparison, String dataFolder, boolean semanticMode,
//			String semanticFolder) {
//		// parse the excel file
//		XLFileHelper xl = new XLFileHelper();
//		xl.parse(excelFile);
//		String sheetName = (String) connectorData.get("worksheet");
//		// put all row data into a List<String[]>
//		List<Object[]> rowData = new ArrayList<>();
//		Object[] row = null;
//		while ((row = xl.getNextRow(sheetName)) != null) {
//			rowData.add(row);
//		}
//		String[] headers = xl.getHeaders(sheetName);
//		List<String> selectedCols = new ArrayList<>();
//		for (String col : dataSelection.keySet()) {
//			// make a list of selected columns
//			Map<String, Object> colInfo = (Map<String, Object>) dataSelection.get(col);
//			for (String cols : colInfo.keySet()) {
//				if ((Boolean) colInfo.get(cols)) {
//					selectedCols.add(cols);
//				}
//			}
//		}
//		for (String col : selectedCols) {
//			// find the index of the selected column from the headers
//			int index = -1;
//			for (String header : headers) {
//				if (header.equalsIgnoreCase(col)) {
//					index = Arrays.asList(headers).indexOf(header);
//				}
//			}
//
//			// get instance values
//			if (index != -1) {
//				HashSet<Object> instances = new HashSet<>();
//				for (int j = 0; j < rowData.size(); j++) {
//					instances.add(rowData.get(j)[index]);
//				}
//				String filePath = dataFolder + "\\" + sheetName + ";" + col + ".txt";
//				filePath = filePath.replace("\\", "/");
//				this.logger.info("Getting " + Utility.cleanLogString(col) + " from excel sheet: " + sheetName + " for X-ray comparison...");
//				// encode and write to file
//				encodeInstances(instances, dataComparison, filePath, semanticMode, semanticFolder);
//			}
//		}
//	}
//
//	/**
//	 * Encrypts data from a csv to files named fileName;column.txt for the
//	 * selected columns
//	 * 
//	 * @param csvFile
//	 *            the location of the csv file
//	 * @param dataSelection
//	 *            determines the specific columns from the csv file to write to
//	 *            text files from the xray config file
//	 * @param dataMode
//	 *            enables datamode compare instance data
//	 * @param dataFolder
//	 *            the location where encrypted data from data mode is written to
//	 * @param semanticMode
//	 *            enables semantic mode to compare wiki header data
//	 * @param semanticFolder
//	 *            the location where data from semantic mode is written to
//	 */
//	private void writeCsvToFile(String csvFile, Map<String, Object> dataSelection, boolean dataMode, String dataFolder,
//			boolean semanticMode, String semanticFolder) {
//		String[] csvFileNameArray = csvFile.split("\\\\");
//		String csvName = csvFileNameArray[csvFileNameArray.length - 1].replace(".csv", "");
//		// read csv into string[]
//		char delimiter = ','; // TODO get from user
//		CSVReader csv = null;
//		try {
//			if (delimiter == '\t') {
//				try {
//					csv = new CSVReader(new FileReader(new File(csvFile)));
//				} catch (FileNotFoundException e) {
//					logger.error(STACKTRACE, e);
//				}
//			} else {
//				try {
//					csv = new CSVReader(new FileReader(new File(csvFile)));
//				} catch (FileNotFoundException e) {
//					logger.error(STACKTRACE, e);
//				}
//			}
//
//			List<String[]> rowData = null;
//
//			if (csv == null) {
//				throw new NullPointerException("csv cannot be null here.");
//			}
//
//			try {
//				rowData = csv.readAll();
//			} catch (IOException e) {
//				logger.error(STACKTRACE, e);
//			}
//
//			if (rowData == null) {
//				throw new NullPointerException("rowData cannot be null here.");
//			}
//
//
//			// get all rows
//			String[] headers = rowData.get(0);
//			List<String> selectedCols = new ArrayList<>();
//			for (String col : dataSelection.keySet()) {
//				// make a list of selected columns
//				Map<String, Object> colInfo = (Map<String, Object>) dataSelection.get(col);
//				for (String cols : colInfo.keySet()) {
//					if ((Boolean) colInfo.get(cols)) {
//						selectedCols.add(cols);
//					}
//				}
//			}
//
//			// iterate through selected columns and only grab those
//			// instances where the indices match
//			for (String col : selectedCols) {
//				// find the index of the selected column in the header
//				// array
//				int index = -1;
//				for (String header : headers) {
//					if (header.equalsIgnoreCase(col)) {
//						index = Arrays.asList(headers).indexOf(header);
//					}
//				}
//
//				// get instance values
//				if (index != -1) {
//					HashSet<Object> instances = new HashSet<>();
//					for (int j = 0; j < rowData.size(); j++) {
//						if (j == 1) {
//							continue;
//						} else {
//							instances.add(rowData.get(j)[index]);
//						}
//					}
//					String fileName = dataFolder + "\\" + csvName + ";" + col + ".txt";
//					fileName = fileName.replace("\\", "/");
//					// encode data to fileName
//					this.logger.info("Getting " + Utility.cleanLogString(col) + " from " + csvName + " csv to run xray comparison... ");
//					encodeInstances(instances, dataMode, fileName, semanticMode, semanticFolder);
//				}
//			}
//		} finally {
//			if(csv != null) {
//		          try {
//		        	  csv.close();
//		          } catch(Exception e) {
//		            logger.error(Constants.STACKTRACE, e);
//		          }
//		        }
//		}
//	}
//
//	/**
//	 * Encrypts data from an external engine to files named
//	 * database;table;column.txt
//	 * 
//	 * @param connectorData
//	 *            used to get the information to connect to the external db from
//	 *            the xray config File
//	 * @param dataSelection
//	 *            determines the specific columns from the external database to
//	 *            write to text files from the xray config file
//	 * @param dataMode
//	 *            enables datamode compare instance data
//	 * @param dataFolder
//	 *            the location where encrypted data from data mode is written to
//	 * @param semanticMode
//	 *            enables semantic mode to compare wiki header data
//	 * @param semanticFolder
//	 *            the location where data from semantic mode is written to
//	 */
//	private void writeExternalToFile(Map<String, Object> connectorData, Map<String, Object> dataSelection,
//			boolean dataMode, String dataFolder, boolean semanticMode, String semanticFolder) {
//		// get external database connection information
//		String connectionUrl = (String) connectorData.get("connectionString");
//		String port = (String) connectorData.get("port");
//		String host = (String) connectorData.get("host");
//		String schema = (String) connectorData.get("schema");
//		String username = (String) connectorData.get("userName");
//		String password = (String) connectorData.get("password");
//		String newDBName = (String) connectorData.get("databaseName");
//		String type = (String) connectorData.get("type");
//		Connection con = null;
//		try {
//			con = RdbmsConnectionHelper.buildConnection(type, host, port, username, password, schema, null);
//		} catch (SQLException e1) {
//			throw new IllegalArgumentException("Invalid connection");
//		}
//		this.logger.info("Querying data from external database " + newDBName + " for X-ray comparison...");
//		// loop through tables within a database
//		for (String table : dataSelection.keySet()) {
//			Map<String, Object> allColumns = (Map<String, Object>) dataSelection.get(table);
//			// loop through columns for a table within a database
//			for (String column : allColumns.keySet()) {
//				Boolean selectedValue = (Boolean) allColumns.get(column);
//				// write the file if the column is selected
//				if (selectedValue) {
//					// build sql query - write only unique values
//					StringBuilder sb = new StringBuilder();
//					sb.append("SELECT DISTINCT ");
//					sb.append(RdbmsQueryBuilder.escapeForSQLStatement(column));
//					sb.append(" FROM ");
//					sb.append(RdbmsQueryBuilder.escapeForSQLStatement(table));
//					sb.append(";");
//					String query = sb.toString();
//					// execute query
//					try(PreparedStatement statement = con.prepareStatement(query)){
//						ResultSet rs = statement.executeQuery();
//						String fileName = dataFolder + "\\" + newDBName + ";" + table + ";" + column + ".txt";
//						fileName = fileName.replace("\\", "/");
//						// get instances
//						List<Object> instances = new ArrayList<>();
//						try {
//							while (rs.next()) {
//								Object value = rs.getString(1);
//								String row = "";
//								if (value != null) {
//									row = ((String) value).replaceAll("\"", "\\\"");
//								}
//								instances.add(row);
//							}
//						} catch (SQLException e) {
//							logger.error(STACKTRACE, e);
//						}
//						// encode and write to file
//						this.logger.info("Querying " + column + " from " + Utility.cleanLogString(table) + " in " + newDBName + " for X-ray comparison...");
//						encodeInstances(instances, dataMode, fileName, semanticMode, semanticFolder);
//					} catch (SQLException e) {
//						logger.error(STACKTRACE, e);
//					}
//				}
//			}
//		}
//		try {
//			con.close();
//		} catch (SQLException e) {
//			logger.error(STACKTRACE, e);
//		}
//	}
//
//	/**
//	 * Encrypts data from a local engine to files named
//	 * database;table;column.txt for the selected columns
//	 * 
//	 * @param connectorData
//	 *            used to get the engineName to connect to the local db from the
//	 *            xray config File
//	 * @param dataSelection
//	 *            determines the specific columns from the database to write to
//	 *            text files from the xray config file
//	 * @param dataMode
//	 *            enables datamode compare instance data
//	 * @param dataFolder
//	 *            the location where encrypted data from data mode is written to
//	 * @param semanticMode
//	 *            enables semantic mode to compare wiki header data
//	 * @param semanticFolder
//	 *            the location where data from semantic mode is written to
//	 */
//	private void writeLocalEngineToFile(Map<String, Object> connectorData, Map<String, Object> dataSelection,
//			boolean dataMode, String dataFolder, boolean semanticMode, String semanticFolder) {
//		String engineID = (String) connectorData.get("engineId");
//		IEngine engine = Utility.getEngine(engineID);
//		// check if engine is valid
//		if (engine != null) {
//			String engineName = engine.getEngineName();
//			this.engineList.add(engineName);
//			this.logger.info("Querying data from local database for X-ray comparison : " + engineName);
//			// loop through tables within a database
//			for (String table : dataSelection.keySet()) {
//				Map<String, Object> allColumns = (Map<String, Object>) dataSelection.get(table);
//				// loop through columns for a table within a database
//				for (String column : allColumns.keySet()) {
//					Boolean selectedValue = (Boolean) allColumns.get(column);
//					// write the file if the column is selected
//					if (selectedValue) {
//						// write concept values
//						if (table.equals(column)) {
//							// get total instance count
//							getLocalEngineInstanceCount(engine, table, null, QueryFunctionHelper.COUNT, false);
//							// write txt file path of where instance data will
//							// be written to
//							// dataFolder/engineName;table;.txt
//							String fileName = dataFolder + "\\" + engineID + ";" + table + ";" + ".txt";
//							fileName = fileName.replace("\\", "/");
//							// get instance data
//							SelectQueryStruct sqs = new SelectQueryStruct();
//							sqs.setEngine(engine);
//							sqs.setEngineId(engineID);
//							sqs.addSelector(table, null);
//							sqs.setDistinct(true);
//
//							List<Object> instances = new ArrayList<>();
//							IRawSelectWrapper iterator = null;
//							try {
//								iterator = WrapperManager.getInstance().getRawWrapper(engine, sqs);
//								while(iterator.hasNext()) {
//									instances.add(iterator.next().getValues()[0]);
//								}
//							} catch (Exception e) {
//								logger.error(STACKTRACE, e);
//							} finally {
//								if(iterator != null) {
//									iterator.cleanUp();
//								}
//							}
//							this.logger.info("Querying table " + Utility.cleanLogString(table) + " from " + engineName + " for X-ray comparison");
//							// encode and write to file
//							encodeInstances(instances, dataMode, fileName, semanticMode, semanticFolder);
//						}
//						// write property values
//						else {
//							// get total instance count
//							getLocalEngineInstanceCount(engine, table, column, QueryFunctionHelper.COUNT, false);
//							// write txt file path of where data will be written
//							// to dataFolder/engineName;table;column.txt
//
//							String fileName = dataFolder + "\\" + engineID + ";" + table + ";" + column + ".txt";
//							fileName = fileName.replace("\\", "/");
//							// get instance data for property
//							SelectQueryStruct sqs = new SelectQueryStruct();
//							sqs.setEngine(engine);
//							sqs.setEngineId(engineID);
//							sqs.addSelector(table, column);
//							sqs.setDistinct(true);
//
//							List<Object> instances = new ArrayList<>(); 
//							IRawSelectWrapper iterator = null;
//							try {
//								iterator = WrapperManager.getInstance().getRawWrapper(engine, sqs);
//								while(iterator.hasNext()) {
//									instances.add(iterator.next().getValues()[0]);
//								}
//							} catch (Exception e) {
//								logger.error(STACKTRACE, e);
//							} finally {
//								if(iterator != null) {
//									iterator.cleanUp();
//								}
//							}
//							this.logger.info("Querying " + column + " from " + Utility.cleanLogString(table) + " in " + engineName + " for X-ray comparison");
//							// encode and write to file
//							encodeInstances(instances, dataMode, fileName, semanticMode, semanticFolder);
//						}
//					}
//				}
//			}
//		}
//	}
//
//	/**
//	 * Write out instance data to file path specified
//	 * 
//	 * @param instances
//	 *            data from source
//	 * @param dataMode
//	 *            encrypts data and compares instance values
//	 * @param filePath
//	 *            where the data to be compared to is written to
//	 * @param semanticMode
//	 *            compares wiki header data
//	 * @param semanticFolder
//	 *            output location of wiki header data to compare
//	 */
//	private void encodeInstances(List<Object> instances, boolean dataMode, String filePath, boolean semanticMode,
//			String semanticFolder) {
//		if (instances.size() > 1) {
//			String[] encodePackages = new String[] {"WikidataR", "WikipediR", "httr", "curl", "jsonlite", "textreuse"};
//
//			// get script to encode instances
//			String minHashFilePath = this.baseFolder + "\\R\\AnalyticsRoutineScripts\\encode_instances.R";
//			minHashFilePath = minHashFilePath.replace("\\", "/");
//			// script used to get semantic data
//			String predictColumnFilePath = this.baseFolder + "\\R\\AnalyticsRoutineScripts\\master_concept.R";
//			predictColumnFilePath = predictColumnFilePath.replace("\\", "/");
//			// load r library and scripts
//			StringBuilder rsb = new StringBuilder();
//			rsb.append(RSyntaxHelper.loadPackages(encodePackages));
//			rsb.append("source(" + "\"" + minHashFilePath + "\"" + ");");
//			rsb.append("source(" + "\"" + predictColumnFilePath + "\"" + ");");
//
//			// write instances to r data frame
//			String dfName = "df.xray" + Utility.getRandomString(8);
//			rsb.append(dfName + "<-data.frame(instances=character(), stringsAsFactors = FALSE);");
//			for (int j = 0; j < instances.size(); j++) {
//				rsb.append(dfName + "[" + (j + 1) + ",1" + "]");
//				rsb.append("<-");
//				if (instances.get(j) == null) {
//					rsb.append("\"" + "" + "\"");
//				} else {
//					// clean up instance values for R to work properly
//					rsb.append("\"" + instances.get(j).toString().replaceAll("[^A-Za-z0-9 ]", "_") + "\"");
//				}
//				rsb.append(";");
//
//			}
//			// generate semantic results and write to semantic folder
//			StringBuilder writeSemanticResultsToFile = new StringBuilder();
//			String semanticResults = "semantic.results.df";
//			if (semanticMode) {
//				String colSelectString = "1";
//				int numDisplay = 3;
//				int randomVals = 20;
//
//				rsb.append(semanticResults + "<- concept_xray(" + dfName + "," + colSelectString + "," + numDisplay
//						+ "," + randomVals + ");");
//				String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
//				String fileCon = Utility.getRandomString(8);
//				writeSemanticResultsToFile.append(fileCon +" <- file(\"" + semanticFolder + "/" + fileName + "\");");
//				writeSemanticResultsToFile.append("writeLines(" + semanticResults + "$Predicted_Concept, "+fileCon+");");
//				writeSemanticResultsToFile.append("close("+fileCon+");");
//				writeSemanticResultsToFile.append("rm("+fileCon+");");
//			}
//			rsb.append(writeSemanticResultsToFile.toString());
//			// encode data frame and write to outputfolder
//			if (dataMode) {
//				rsb.append("encode_instances(" + dfName + "," + "\"" + filePath + "\"" + ");");
//			}
//			// clean up r temp variables
//			rsb.append("rm(" + dfName + ",concept_mgr, concept_xray, encode_instances, get_claims, get_concept, get_wiki_ids, is.letter, most_frequent_concept, span);");
//			if (semanticMode) {
//				rsb.append("rm(" + semanticResults + ");");
//			}
//			// run r script
//			this.rJavaTranslator.runR(rsb.toString());
//		}
//	}
//
//	/**
//	 * Used to encode instances from csv or excel file
//	 * 
//	 * @param instances
//	 *            data from source
//	 * @param dataMode
//	 *            encrypts data and compares instance values
//	 * @param filePath
//	 *            where the data to be compared to is written to
//	 * @param semanticMode
//	 *            compares wiki header data
//	 * @param semanticFolder
//	 *            output location of wiki header data to compare
//	 */
//	private void encodeInstances(HashSet<Object> instances, boolean dataMode, String filePath, boolean semanticMode,
//			String semanticFolder) {
//		if (instances.size() > 1) {
//			String[] encodePackages = new String[] {"WikidataR", "WikipediR", "httr", "curl", "jsonlite", "textreuse"};
//
//			String minHashFilePath = this.baseFolder + "\\" + Constants.R_BASE_FOLDER + "\\"
//					+ Constants.R_ANALYTICS_SCRIPTS_FOLDER + "\\" + "encode_instances.R";
//			minHashFilePath = minHashFilePath.replace("\\", "/");
//
//			StringBuilder rsb = new StringBuilder();
//			// load packages
//			rsb.append(RSyntaxHelper.loadPackages(encodePackages));
//			rsb.append("source(" + "\"" + minHashFilePath + "\"" + ");");
//
//			// construct R dataframe
//			String dfName = "df.xray";
//			rsb.append(dfName + "<-data.frame(instances=character(), stringsAsFactors = FALSE);");
//			int j = 0;
//			for (Object value : instances) {
//				rsb.append(dfName + "[" + (j + 1) + ",1" + "]");
//				rsb.append("<-");
//				if (value == null) {
//					rsb.append("\"" + "" + "\"");
//				} else {
//					// clean up instance values for R to work properly
//					rsb.append("\"" + value.toString().replaceAll("[^A-Za-z0-9 ]", "_") + "\"");
//				}
//				rsb.append(";");
//				j++;
//
//			}
//			// run predict column headers write output to folder
//			StringBuilder writeFrameResultsToFile = new StringBuilder();
//			if (semanticMode) {
//				String semanticResults = "semantic.results.df";
//				String colSelectString = "1";
//				int numDisplay = 3;
//				int randomVals = 20;
//
//				rsb.append(semanticResults + "<- concept_xray(" + dfName + "," + colSelectString + "," + numDisplay
//						+ "," + randomVals + ");");
//				String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
//				String fileCon = Utility.getRandomString(8);
//				writeFrameResultsToFile.append(fileCon + " <- file(\"" + semanticFolder + "/" + fileName + "\");");
//				writeFrameResultsToFile.append("writeLines(" + semanticResults + "$Predicted_Concept, " + fileCon + ");");
//				writeFrameResultsToFile.append("close(" + fileCon + ");");
//				writeFrameResultsToFile.append("rm(" + fileCon + ")");
//			}
//			rsb.append(writeFrameResultsToFile.toString());
//			if (dataMode) {
//				rsb.append("encode_instances(" + dfName + "," + "\"" + filePath + "\"" + ");");
//			}
//			// clean up r temp variables
//			rsb.append("rm(" + dfName + ",concept_mgr, concept_xray, encode_instances, get_claims, get_concept, get_wiki_ids, is.letter, most_frequent_concept, span);");
//			this.rJavaTranslator.runR(rsb.toString());
//
//		}
//	}
//
//	/**
//	 * Get xray param to set data mode
//	 * 
//	 * @param parameters
//	 * @return
//	 */
//	private boolean getDataMode(Map<String, Object> parameters) {
//		boolean dataMode = false;
//		Boolean dataParam = (Boolean) parameters.get("dataMode");
//		if (dataParam != null) {
//			dataMode = dataParam.booleanValue();
//		}
//		return dataMode;
//	}
//
//	/**
//	 * Get xray param to set semantic mode
//	 * 
//	 * @param parameters
//	 * @return
//	 */
//	private boolean getSemanticMode(Map<String, Object> parameters) {
//		boolean semanticMode = false;
//		Boolean semanticParam = (Boolean) parameters.get("semanticMode");
//		if (semanticParam != null) {
//			semanticMode = semanticParam.booleanValue();
//		}
//		return semanticMode;
//	}
//
//	/**
//	 * Get xray param to match a database to itself
//	 * 
//	 * @param parameters
//	 * @return matchSameDB
//	 */
//	private Boolean getMatchSameDB(Map<String, Object> parameters) {
//		Boolean matchDB = (Boolean) parameters.get("matchSameDb");
//		if (matchDB == null) {
//			matchDB = false;
//		}
//		return matchDB;
//	}
//
//	/**
//	 * Get xray param to set the candidate threshold to match data
//	 * 
//	 * @param parameters
//	 * @return candidateThreshold
//	 */
//	private double getCandidateThreshold(Map<String, Object> parameters) {
//		double candidateThreshold = -1;
//		Object cand = parameters.get("candidate");
//		Double candidate = null;
//		if (cand instanceof Integer) {
//			candidate = (double) ((Integer) cand).intValue();
//		} else {
//			candidate = (Double) cand;
//		}
//		if (candidate != null) {
//			candidateThreshold = candidate.doubleValue();
//		}
//		if (candidateThreshold < 0 || candidateThreshold > 1) {
//			candidateThreshold = 0.01;
//		}
//		return candidateThreshold;
//	}
//
//	/**
//	 * Get xray param to get similarity threshold
//	 * 
//	 * @param parameters
//	 * @return
//	 */
//	private double getSimiliarityThreshold(Map<String, Object> parameters) {
//		double similarityThreshold = -1;
//		Object sim = parameters.get("similarity");
//		Double similarity = null;
//		if (sim instanceof Integer) {
//			similarity = (double) ((Integer) sim).intValue();
//		} else {
//			similarity = (Double) sim;
//		}
//		if (similarity != null) {
//			similarityThreshold = similarity.doubleValue();
//		}
//		if (similarityThreshold < 0 || similarityThreshold > 1) {
//			similarityThreshold = 0.01;
//		}
//		return similarityThreshold;
//	}
//
//	/**
//	 * Get Local engine instance and save to lists to write to R dataframe if
//	 * xray generateCountFrame is enabled
//	 * 
//	 * @param engine
//	 * @param concept
//	 * @param prop
//	 * @param functionName
//	 * @param distinct
//	 * @return
//	 */
//	private void getLocalEngineInstanceCount(IEngine engine, String concept, String prop, String functionName, boolean distinct) {
//		if (this.genCountFrame) {
//			SelectQueryStruct qs2 = new SelectQueryStruct();
//			{
//				QueryFunctionSelector funSelector = new QueryFunctionSelector();
//				funSelector.setFunction(functionName);
//				QueryColumnSelector innerSelector = new QueryColumnSelector();
//				innerSelector.setTable(concept);
//				// concept
//				if (prop == null) {
//					innerSelector.setColumn(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER);
//				} else {
//					innerSelector.setColumn(prop);
//				}
//				funSelector.addInnerSelector(innerSelector);
//				funSelector.setDistinct(distinct);
//				qs2.addSelector(funSelector);
//			}
//			qs2.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE);
//
//			Integer count = 0;
//			IRawSelectWrapper it = null;
//			try {
//				it = WrapperManager.getInstance().getRawWrapper(engine, qs2);
//				if (it.hasNext()) {
//					Number num =(Number) it.next().getValues()[0];
//					count = num.intValue();
//				}
//			} catch (Exception e) {
//				logger.error(STACKTRACE, e);
//			} finally {
//				if(it != null) {
//					it.cleanUp();
//				}
//			}
//			engineColumn.add(engine.getEngineId());
//			tableColumn.add(concept);
//			if (prop == null) {
//				propColumn.add(concept);
//			} else {
//				propColumn.add(prop);
//			}
//			countColumn.add(count);
//		}
//	}
//	/**
//	 * @return get countDF
//	 */
//	public String getCountDF() {
//		return this.countDF;
//	}
//	public void setGenerateCountFrame(boolean countDF){
//		this.genCountFrame = countDF;
//	}
//	public Set<String> getEngineList() {
//		return this.engineList;
//	}
//}