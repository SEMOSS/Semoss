package prerna.sablecc2.reactor.algorithms.xray;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.codehaus.jackson.map.ObjectMapper;

import au.com.bytecode.opencsv.CSVReader;
import prerna.algorithm.learning.matching.DomainValues;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.poi.main.helper.XLFileHelper;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.reactor.frame.r.GenerateH2FrameFromRVariableReactor;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.ga.GATracker;

/**
 * Writes instance data from CSV, EXCEL, LOCAL and EXTERNAL datasources to txt
 * files and compares them using R to find matching columns across
 * 
 * There are two modes to run R comparison
 * 
 * Data mode: compares encrypted instance data across datasources txt files are
 * written to SemossBase\R\XrayCompatibility\Temp\MatchingRepository
 * 
 * Semantic mode: predicts instance data headers based on wikipedia and compares
 * them across datasources files are written to
 * SemossBase\R\XrayCompatibility\Temp\SemanticRepository
 * 
 * RunXray("xrayConfigFile");
 */
public class XRayReactor extends AbstractRFrameReactor {
	public XRayReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.CONFIG_FILE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		// need to make sure that the textreuse package is installed
		String hasPackage = this.rJavaTranslator
				.getString("as.character(\"textreuse\" %in% rownames(installed.packages()))");
		if (!hasPackage.equalsIgnoreCase("true")) {
			throw new IllegalArgumentException("The textreuse package is NOT installed");
		}
		organizeKeys();
		String configFileJson = this.keyValue.get(this.keysToGet[0]);
		if (configFileJson == null) {
			throw new IllegalArgumentException("Need to define the config file");
		}

		HashMap<String, Object> config = null;
		try {
			config = new ObjectMapper().readValue(configFileJson, HashMap.class);
		} catch (IOException e2) {
			throw new IllegalArgumentException("Invalid xray config json file ");
		}
		// output is written to this h2 frame
		H2Frame frame = (H2Frame) getFrame();
		// output folder for data mode to be written to
		String dataFolder = getBaseFolder() + "\\R\\XrayCompatibility\\Temp\\MatchingRepository";
		dataFolder = dataFolder.replace("\\", "/");
		// output folder for semantic data to be written to
		String semanticFolder = getBaseFolder() + "\\R\\XrayCompatibility\\Temp\\SemanticRepository";
		semanticFolder = semanticFolder.replace("\\", "/");
		// clean output folders
		try {
			FileUtils.cleanDirectory(new File(dataFolder));
			FileUtils.cleanDirectory(new File(semanticFolder));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// get xray parameters
		HashMap<String, Object> parameters = (HashMap<String, Object>) config.get("parameters");
		boolean semanticMode = getSemanticMode(parameters);
		boolean dataMode = getDataMode(parameters);
		// if no mode is defined set xray to data mode
		if ((!semanticMode && !dataMode)) {
			dataMode = true;
		}

		// Write text files to run xray comparison from different sources
		// outputFolder/database;table;column.txt
		ArrayList<Object> connectors = (ArrayList<Object>) config.get("connectors");
		for (int i = 0; i < connectors.size(); i++) {
			HashMap<String, Object> connection = (HashMap<String, Object>) connectors.get(i);
			String connectorType = (String) connection.get("connectorType");
			HashMap<String, Object> connectorData = (HashMap<String, Object>) connection.get("connectorData");
			HashMap<String, Object> dataSelection = (HashMap<String, Object>) connection.get("dataSelection");
			if (connectorType.toUpperCase().equals("LOCAL")) {
				writeLocalEngineToFile(connectorData, dataSelection, dataMode, dataFolder, semanticMode,
						semanticFolder);
			} else if (connectorType.toUpperCase().equals("EXTERNAL")) {
				writeExternalToFile(connectorData, dataSelection, dataMode, dataFolder, semanticMode, semanticFolder);
			} else if (connectorType.toUpperCase().equals("FILE")) {
				// process csv file reading
				String sourceFile = (String) connectorData.get("filePath");
				String extension = FilenameUtils.getExtension(sourceFile);
				if (extension.equals("csv") || extension.equals("txt")) {
					writeCsvToFile(sourceFile, dataSelection, dataMode, dataFolder, semanticMode, semanticFolder);
				} else if (extension.equals("xls") || extension.equals("xlsx")) {
					writeExcelToFile(sourceFile, dataSelection, connectorData, dataMode, dataFolder, semanticMode,
							semanticFolder);
				}
			}
		}

		// get other parameters for xray script
		int nMinhash = 0;
		int nBands = 0;
		int instancesThreshold = 1;
		double similarityThreshold = getSimiliarityThreshold(parameters);
		double candidateThreshold = getCandidateThreshold(parameters);
		// check if user wants to compare columns from the same database
		// this is the boolean value passed into R script
		Boolean matchSameDB = getMatchSameDB(parameters);
		if (candidateThreshold <= 0.03) {
			nMinhash = 3640;
			nBands = 1820;
		} else if (candidateThreshold <= 0.02) {
			nMinhash = 8620;
			nBands = 4310;
		} else if (candidateThreshold <= 0.01) {
			nMinhash = 34480;
			nBands = 17240;
		} else if (candidateThreshold <= 0.05) {
			nMinhash = 1340;
			nBands = 670;
		} else if (candidateThreshold <= 0.1) {
			nMinhash = 400;
			nBands = 200;
		} else if (candidateThreshold <= 0.2) {
			nMinhash = 200;
			nBands = 100;
		} else if (candidateThreshold <= 0.4) {
			nMinhash = 210;
			nBands = 70;
		} else if (candidateThreshold <= 0.5) {
			nMinhash = 200;
			nBands = 50;
		} else {
			nMinhash = 200;
			nBands = 40;
		}

		String baseMatchingFolder = getBaseFolder() + "\\R\\XrayCompatibility";
		baseMatchingFolder = baseMatchingFolder.replace("\\", "/");
		// print xray data mode results to csv
		// Semoss/R/Matching/Temp/rdbms
		String outputXrayDataFolder = baseMatchingFolder + "\\Temp\\rdbms";
		outputXrayDataFolder = outputXrayDataFolder.replace("\\", "/");

		// Parameters for R script
		String rFrameName = "xray";

		// Grab the utility script
		String utilityScriptPath = baseMatchingFolder + "\\" + "matching.R";
		utilityScriptPath = utilityScriptPath.replace("\\", "/");

		// Create R Script to run xray
		StringBuilder rsb = new StringBuilder();
		rsb.append("library(textreuse);");

		// Source the LSH function from the utility script
		rsb.append("source(\"" + utilityScriptPath + "\");");
		rsb.append(rFrameName + " <- data.frame();");

		// Run locality sensitive hashing to generate matches
		if (dataMode) {
			rsb.append(rFrameName + " <- " + Constants.R_LSH_MATCHING_FUN + "(\"" + dataFolder + "\", " + nMinhash
					+ ", " + nBands + ", " + similarityThreshold + ", " + instancesThreshold + ", \""
					+ DomainValues.ENGINE_CONCEPT_PROPERTY_DELIMETER + "\", " + matchSameDB.toString().toUpperCase()
					+ ", \"" + outputXrayDataFolder + "\");");
		}
		this.rJavaTranslator.runR(rsb.toString());

		// run xray on semantic folder
		if (semanticMode) {
			rsb = new StringBuilder();
			String semanticComparisonFrame = "semantic.xray.df";
			rsb.append("library(textreuse);");

			// Source the LSH function from the utility script
			rsb.append("source(\"" + utilityScriptPath + "\");");
			rsb.append(semanticComparisonFrame + " <- data.frame();");
			String semanticOutputFolder = baseMatchingFolder + "\\Temp\\semantic";
			semanticOutputFolder = semanticOutputFolder.replace("\\", "/");
			rsb.append(semanticComparisonFrame + " <- " + Constants.R_LSH_MATCHING_FUN + "(\"" + semanticFolder + "\", "
					+ nMinhash + ", " + nBands + ", " + similarityThreshold + ", " + instancesThreshold + ", \""
					+ DomainValues.ENGINE_CONCEPT_PROPERTY_DELIMETER + "\", " + matchSameDB.toString().toUpperCase()
					+ ", \"" + semanticOutputFolder + "\");");

			// join data xray df with semantic xray df if dataComparison frame
			// was created
			if (dataMode) {
				String mergeRScriptPath = baseMatchingFolder + "\\merge.r";
				mergeRScriptPath = mergeRScriptPath.replace("\\", "/");
				rsb.append("source(\"" + mergeRScriptPath + "\");");
				rsb.append(rFrameName + " <- xray_merge(" + rFrameName + ", " + semanticComparisonFrame + ");");
			} else {
				rsb.append(rFrameName + " <-" + semanticComparisonFrame + ";");
			}
			this.rJavaTranslator.runR(rsb.toString());
		}

		// Synchronize from R
		GenerateH2FrameFromRVariableReactor sync = new GenerateH2FrameFromRVariableReactor();
		sync.syncFromR(this.rJavaTranslator, rFrameName, frame);
		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.CODE_EXECUTION);
		
		// track GA data
		GATracker.getInstance().trackAnalyticsPixel(this.insight, "XRay");
		
		return noun;
	}

	/**
	 * Encrypts data from an excel file to files named sheetName;column.txt for
	 * the selected columns
	 * 
	 * @param excelFile
	 *            the location of the excelFile
	 * @param dataSelection
	 *            determines the specific columns from the excel file to write
	 *            to text files from the xray config file
	 * @param dataMode
	 *            enables data mode compare instance data
	 * @param dataFolder
	 *            the location where encrypted data from data mode is written to
	 * @param semanticMode
	 *            enables semantic mode to compare wiki header data
	 * @param semanticFolder
	 *            the location where data from semantic mode is written to
	 */
	private void writeExcelToFile(String excelFile, HashMap<String, Object> dataSelection,
			HashMap<String, Object> connectorData, boolean dataComparison, String dataFolder, boolean semanticMode,
			String semanticFolder) {
		// parse the excel file
		XLFileHelper xl = new XLFileHelper();
		xl.parse(excelFile);
		String sheetName = (String) connectorData.get("worksheet");
		// put all row data into a List<String[]>
		List<String[]> rowData = new ArrayList<String[]>();
		String[] row = null;
		while ((row = xl.getNextRow(sheetName)) != null) {
			rowData.add(row);
		}
		String[] headers = xl.getHeaders(sheetName);
		List<String> selectedCols = new ArrayList<String>();
		for (String col : dataSelection.keySet()) {
			// make a list of selected columns
			HashMap<String, Object> colInfo = (HashMap<String, Object>) dataSelection.get(col);
			for (String cols : colInfo.keySet()) {
				if ((Boolean) colInfo.get(cols) == true) {
					selectedCols.add(cols);
				}
			}
		}
		for (String col : selectedCols) {
			// find the index of the selected column from the headers
			int index = -1;
			for (String header : headers) {
				if (header.toUpperCase().equals(col.toUpperCase())) {
					index = Arrays.asList(headers).indexOf(header);
				}
			}

			// get instance values
			if (index != -1) {
				HashSet<Object> instances = new HashSet<Object>();
				for (int j = 0; j < rowData.size(); j++) {
					instances.add(rowData.get(j)[index]);
				}
				String filePath = dataFolder + "\\" + sheetName + ";" + col + ".txt";
				filePath = filePath.replace("\\", "/");
				// encode and write to file
				encodeInstances(instances, dataComparison, filePath, semanticMode, semanticFolder);
			}
		}
	}

	/**
	 * Encrypts data from a csv to files named fileName;column.txt for the
	 * selected columns
	 * 
	 * @param csvFile
	 *            the location of the csv file
	 * @param dataSelection
	 *            determines the specific columns from the csv file to write to
	 *            text files from the xray config file
	 * @param dataMode
	 *            enables datamode compare instance data
	 * @param dataFolder
	 *            the location where encrypted data from data mode is written to
	 * @param semanticMode
	 *            enables semantic mode to compare wiki header data
	 * @param semanticFolder
	 *            the location where data from semantic mode is written to
	 */
	private void writeCsvToFile(String csvFile, HashMap<String, Object> dataSelection, boolean dataMode,
			String dataFolder, boolean semanticMode, String semanticFolder) {
		String[] csvFileNameArray = csvFile.split("\\\\");
		String csvName = csvFileNameArray[csvFileNameArray.length - 1].replace(".csv", "");
		// read csv into string[]
		char delimiter = ','; // TODO get from user
		CSVReader csv = null;
		if (delimiter == '\t') {
			try {
				csv = new CSVReader(new FileReader(new File(csvFile)));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			try {
				csv = new CSVReader(new FileReader(new File(csvFile)));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

		List<String[]> rowData = null;
		try {
			rowData = csv.readAll();
		} catch (IOException e) {
			e.printStackTrace();
		} // get all rows
		String[] headers = rowData.get(0);
		List<String> selectedCols = new ArrayList<String>();
		for (String col : dataSelection.keySet()) {
			// make a list of selected columns
			HashMap<String, Object> colInfo = (HashMap<String, Object>) dataSelection.get(col);
			for (String cols : colInfo.keySet()) {
				if ((Boolean) colInfo.get(cols) == true) {
					selectedCols.add(cols);
				}
			}
		}

		// iterate through selected columns and only grab those
		// instances where the indices match
		for (String col : selectedCols) {
			// find the index of the selected column in the header
			// array
			int index = -1;
			for (String header : headers) {
				if (header.toUpperCase().equals(col.toUpperCase())) {
					index = Arrays.asList(headers).indexOf(header);
				}
			}

			// get instance values
			if (index != -1) {
				HashSet<Object> instances = new HashSet<Object>();
				for (int j = 0; j < rowData.size(); j++) {
					if (j == 1) {
						continue;
					} else {
						instances.add(rowData.get(j)[index]);
					}
				}
				String fileName = dataFolder + "\\" + csvName + ";" + col + ".txt";
				fileName = fileName.replace("\\", "/");
				// encode data to fileName
				encodeInstances(instances, dataMode, fileName, semanticMode, semanticFolder);
			}
		}
	}

	/**
	 * Encrypts data from an external engine to files named
	 * database;table;column.txt
	 * 
	 * @param connectorData
	 *            used to get the information to connect to the external db from
	 *            the xray config File
	 * @param dataSelection
	 *            determines the specific columns from the external database to
	 *            write to text files from the xray config file
	 * @param dataMode
	 *            enables datamode compare instance data
	 * @param dataFolder
	 *            the location where encrypted data from data mode is written to
	 * @param semanticMode
	 *            enables semantic mode to compare wiki header data
	 * @param semanticFolder
	 *            the location where data from semantic mode is written to
	 */
	private void writeExternalToFile(HashMap<String, Object> connectorData, HashMap<String, Object> dataSelection,
			boolean dataMode, String dataFolder, boolean semanticMode, String semanticFolder) {
		// get external database connection information
		String connectionUrl = (String) connectorData.get("connectionString");
		String port = (String) connectorData.get("port");
		String host = (String) connectorData.get("host");
		String schema = (String) connectorData.get("schema");
		String username = (String) connectorData.get("userName");
		String password = (String) connectorData.get("password");
		String newDBName = (String) connectorData.get("databaseName");
		String type = (String) connectorData.get("type");
		Connection con = null;
		try {
			con = RdbmsConnectionHelper.buildConnection(type, host, port, username, password, schema, null);
		} catch (SQLException e1) {
			throw new IllegalArgumentException("Invalid connection");
		}
		// loop through tables within a database
		for (String table : dataSelection.keySet()) {
			HashMap<String, Object> allColumns = (HashMap<String, Object>) dataSelection.get(table);
			// loop through columns for a table within a database
			for (String column : allColumns.keySet()) {
				Boolean selectedValue = (Boolean) allColumns.get(column);
				// write the file if the column is selected
				if (selectedValue) {
					// build sql query - write only unique values
					StringBuilder sb = new StringBuilder();
					sb.append("SELECT DISTINCT ");
					sb.append(column);
					sb.append(" FROM ");
					sb.append(table);
					sb.append(";");
					String query = sb.toString();
					// execute query
					Statement stmt = null;
					try {
						stmt = con.createStatement();
						ResultSet rs = stmt.executeQuery(query);
						String fileName = dataFolder + "\\" + newDBName + ";" + table + ";" + column + ".txt";
						fileName = fileName.replace("\\", "/");
						// get instances
						List<Object> instances = new ArrayList<Object>();
						try {
							while (rs.next()) {
								Object value = rs.getString(1);
								String row = "";
								if (value != null) {
									row = ((String) value).replaceAll("\"", "\\\"");
								}
								instances.add(row.toString());
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}
						// encode and write to file
						encodeInstances(instances, dataMode, fileName, semanticMode, semanticFolder);
						stmt.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
		try {
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Encrypts data from a local engine to files named
	 * database;table;column.txt for the selected columns
	 * 
	 * @param connectorData
	 *            used to get the engineName to connect to the local db from the
	 *            xray config File
	 * @param dataSelection
	 *            determines the specific columns from the database to write to
	 *            text files from the xray config file
	 * @param dataMode
	 *            enables datamode compare instance data
	 * @param dataFolder
	 *            the location where encrypted data from data mode is written to
	 * @param semanticMode
	 *            enables semantic mode to compare wiki header data
	 * @param semanticFolder
	 *            the location where data from semantic mode is written to
	 */
	private void writeLocalEngineToFile(HashMap<String, Object> connectorData, HashMap<String, Object> dataSelection,
			boolean dataMode, String dataFolder, boolean semanticMode, String semanticFolder) {
		String engineName = (String) connectorData.get("engineName");
		IEngine engine = Utility.getEngine(engineName);
		// check if engine is valid
		if (engine != null) {
			// loop through tables within a database
			for (String table : dataSelection.keySet()) {
				HashMap<String, Object> allColumns = (HashMap<String, Object>) dataSelection.get(table);
				// loop through columns for a table within a database
				for (String column : allColumns.keySet()) {
					Boolean selectedValue = (Boolean) allColumns.get(column);
					// write the file if the column is selected
					if (selectedValue) {
						// write concept values
						if (table.equals(column)) {
							// write txt file path of where instance data will
							// be written to
							// dataFolder/engineName;table;.txt
							String fileName = dataFolder + "\\" + engineName + ";" + table + ";" + ".txt";
							fileName = fileName.replace("\\", "/");
							// get instance data
							String uri = DomainValues.getConceptURI(table, engine, true);
							List<Object> instances;
							if (engine.getEngineType().equals(IEngine.ENGINE_TYPE.SESAME)) {
								instances = DomainValues.retrieveCleanConceptValues(uri, engine);
							} else {
								instances = DomainValues.retrieveCleanConceptValues(table, engine);
							}
							// encode and write to file
							encodeInstances(instances, dataMode, fileName, semanticMode, semanticFolder);
						}
						// write property values
						else {
							// write txt file path of where data will be written
							// to dataFolder/engineName;table;column.txt
							String fileName = dataFolder + "\\" + engineName + ";" + table + ";" + column + ".txt";
							fileName = fileName.replace("\\", "/");
							// get instance data for property
							String conceptUri = DomainValues.getConceptURI(table, engine, true);
							String propUri = DomainValues.getPropertyURI(column, table, engine, false);
							List<Object> instances;
							if (engine.getEngineType().equals(IEngine.ENGINE_TYPE.SESAME)) {
								instances = DomainValues.retrieveCleanPropertyValues(conceptUri, propUri, engine);
							} else {
								instances = DomainValues.retrieveCleanPropertyValues(conceptUri, propUri, engine);
							}
							// encode and write to file
							encodeInstances(instances, dataMode, fileName, semanticMode, semanticFolder);
						}
					}
				}
			}
		}
	}

	/**
	 * Write out instance data to file path specified
	 * 
	 * @param instances
	 *            data from source
	 * @param dataMode
	 *            encrypts data and compares instance values
	 * @param filePath
	 *            where the data to be compared to is written to
	 * @param semanticMode
	 *            compares wiki header data
	 * @param semanticFolder
	 *            output location of wiki header data to compare
	 */
	private void encodeInstances(List<Object> instances, boolean dataMode, String filePath, boolean semanticMode,
			String semanticFolder) {
		if (instances.size() > 1) {
			// get script to encode instances
			String minHashFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\encode_instances.r";
			minHashFilePath = minHashFilePath.replace("\\", "/");
			// script used to get semantic data
			String predictColumnFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\master_concept.r";
			predictColumnFilePath = predictColumnFilePath.replace("\\", "/");
			// load r library and scripts
			StringBuilder rsb = new StringBuilder();
			rsb.append("library(textreuse);");
			rsb.append("source(" + "\"" + minHashFilePath + "\"" + ");");
			rsb.append("source(" + "\"" + predictColumnFilePath + "\"" + ");");

			// write instances to r data frame
			String dfName = "df.xray";
			rsb.append(dfName + "<-data.frame(instances=character(), stringsAsFactors = FALSE);");
			for (int j = 0; j < instances.size(); j++) {
				rsb.append(dfName + "[" + (j + 1) + ",1" + "]");
				rsb.append("<-");
				if (instances.get(j) == null) {
					rsb.append("\"" + "" + "\"");
				} else {
					rsb.append("\"" + instances.get(j).toString() + "\"");
				}
				rsb.append(";");

			}
			// generate semantic results and write to semantic folder
			String writeSemanticResultsToFile = "";
			if (semanticMode) {
				String semanticResults = "semantic.results.df";
				String colSelectString = "1";
				int numDisplay = 3;
				int randomVals = 20;

				rsb.append(semanticResults + "<- concept_xray(" + dfName + "," + colSelectString + "," + numDisplay
						+ "," + randomVals + ");");
				String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
				writeSemanticResultsToFile = "fileConn <- file(\"" + semanticFolder + "/" + fileName + "\");";
				writeSemanticResultsToFile += "writeLines(" + semanticResults + "$Predicted_Concept, fileConn);";
				writeSemanticResultsToFile += "close(fileConn);";
			}
			rsb.append(writeSemanticResultsToFile);
			// encode data frame and write to outputfolder
			if (dataMode) {
				rsb.append("encode_instances(" + dfName + "," + "\"" + filePath + "\"" + ");");
			}
			// run r script
			this.rJavaTranslator.runR(rsb.toString());
		}
	}

	/**
	 * Used to encode instances from csv or excel file
	 * 
	 * @param instances
	 *            data from source
	 * @param dataMode
	 *            encrypts data and compares instance values
	 * @param filePath
	 *            where the data to be compared to is written to
	 * @param semanticMode
	 *            compares wiki header data
	 * @param semanticFolder
	 *            output location of wiki header data to compare
	 */
	private void encodeInstances(HashSet<Object> instances, boolean dataMode, String filePath, boolean semanticMode,
			String semanticFolder) {
		if (instances.size() > 1) {
			String minHashFilePath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\"
					+ Constants.R_ANALYTICS_SCRIPTS_FOLDER + "\\" + "encode_instances.r";
			minHashFilePath = minHashFilePath.replace("\\", "/");

			StringBuilder rsb = new StringBuilder();
			rsb.append("library(textreuse);");
			rsb.append("source(" + "\"" + minHashFilePath + "\"" + ");");

			// construct R dataframe
			String dfName = "df.xray";
			rsb.append(dfName + "<-data.frame(instances=character(), stringsAsFactors = FALSE);");
			int j = 0;
			for (Object value : instances) {
				rsb.append(dfName + "[" + (j + 1) + ",1" + "]");
				rsb.append("<-");
				if (value == null) {
					rsb.append("\"" + "" + "\"");
				} else {
					rsb.append("\"" + value.toString() + "\"");
				}
				rsb.append(";");
				j++;

			}
			// run predict column headers write output to folder
			String writeFrameResultsToFile = "";
			if (semanticMode) {
				String semanticResults = "semantic.results.df";
				String colSelectString = "1";
				int numDisplay = 3;
				int randomVals = 20;

				rsb.append(semanticResults + "<- concept_xray(" + dfName + "," + colSelectString + "," + numDisplay
						+ "," + randomVals + ");");
				String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);
				writeFrameResultsToFile = "fileConn <- file(\"" + semanticFolder + "/" + fileName + "\");";
				writeFrameResultsToFile += "writeLines(" + semanticResults + "$Predicted_Concept, fileConn);";
				writeFrameResultsToFile += "close(fileConn);";
			}
			rsb.append(writeFrameResultsToFile);
			if (dataMode) {
				rsb.append("encode_instances(" + dfName + "," + "\"" + filePath + "\"" + ");");
			}
			this.rJavaTranslator.runR(rsb.toString());
		}
	}

	/**
	 * Get xray param to match a database to itself
	 * 
	 * @param parameters
	 * @return matchSameDB
	 */
	private Boolean getMatchSameDB(HashMap<String, Object> parameters) {
		Boolean matchDB = (Boolean) parameters.get("matchSameDb");
		if (matchDB == null) {
			matchDB = false;
		}
		return matchDB;
	}

	/**
	 * Get xray param to set the candidate threshold to match data
	 * 
	 * @param parameters
	 * @return candidateThreshold
	 */
	private double getCandidateThreshold(HashMap<String, Object> parameters) {
		double candidateThreshold = -1;
		Object cand = parameters.get("candidate");
		Double candidate = null;
		if (cand instanceof Integer) {
			candidate = (double) ((Integer) cand).intValue();
		} else {
			candidate = (Double) cand;
		}
		if (candidate != null) {
			candidateThreshold = candidate.doubleValue();
		}
		if (candidateThreshold < 0 || candidateThreshold > 1) {
			candidateThreshold = 0.01;
		}
		return candidateThreshold;
	}

	/**
	 * Get xray param to get similarity threshold
	 * 
	 * @param parameters
	 * @return
	 */
	private double getSimiliarityThreshold(HashMap<String, Object> parameters) {
		double similarityThreshold = -1;
		Object sim = parameters.get("similarity");
		Double similarity = null;
		if (sim instanceof Integer) {
			similarity = (double) ((Integer) sim).intValue();
		} else {
			similarity = (Double) sim;
		}
		if (similarity != null) {
			similarityThreshold = similarity.doubleValue();
		}
		if (similarityThreshold < 0 || similarityThreshold > 1) {
			similarityThreshold = 0.01;
		}
		return similarityThreshold;
	}

	/**
	 * Get xray param to set data mode
	 * 
	 * @param parameters
	 * @return
	 */
	private boolean getDataMode(HashMap<String, Object> parameters) {
		boolean dataMode = false;
		Boolean dataParam = (Boolean) parameters.get("dataMode");
		if (dataParam != null) {
			dataMode = dataParam.booleanValue();
		}
		return dataMode;
	}

	/**
	 * Get xray param to set semantic mode
	 * 
	 * @param parameters
	 * @return
	 */
	private boolean getSemanticMode(HashMap<String, Object> parameters) {
		boolean semanticMode = false;
		Boolean semanticParam = (Boolean) parameters.get("semanticMode");
		if (semanticParam != null) {
			semanticMode = semanticParam.booleanValue();
		}
		return semanticMode;
	}

}
