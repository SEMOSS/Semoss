package prerna.reactor.frame.r.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractRJavaTranslator implements IRJavaTranslator {

	private static final Logger classLogger = LogManager.getLogger(AbstractRJavaTranslator.class);

	Insight insight = null;
	Logger logger = null;
	public String env = "environment()";
	
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////

	/*
	 * abstract internal methods
	 * should not be used outside of the translator
	 */

	/**
	 * Execute an R Script without a return
	 * FOR INTERNAL USE ONLY - IF YOU TRY TO CALL IT WONT WORK
	 * @param rScript
	 */
	abstract void executeEmptyRDirect(String rScript);
	
	/**
	 * Execute an R Script
	 * FOR INTERNAL USE ONLY - IF YOU TRY TO CALL IT WONT WORK
	 * @param rScript
	 */
	abstract Object executeRDirect(String rScript);
	
	
	/*
	 * public abstract methods defined here
	 */
	
	/**
	 * This method uses specific Rserve or JRI methods to get the breaks and counts for a histogram
	 * breaks for a histogram as double[]
	 * counts for a histogram as int[]
	 * Map keys are breaks and counts
	 * @param script
	 */
	public abstract Map<String, Object> getHistogramBreaksAndCounts(String script);
	
	/**
	 * This method uses specific Rserve or JRI methods to get a table in the form Map<String, Object>
	 * @param framename
	 * @param colNames
	 */
	public abstract Map<String, Object> flushFrameAsTable(String framename, String[] colNames);
	
	/**
	 * Execute to get a row of data
	 * @param rScript
	 * @param headerOrdering
	 * @return
	 */
	public abstract Object[] getDataRow(String rScript, String[] headerOrdering);
	
	/**
	 * Execute to get a list of data rows
	 * @param rScript
	 * @param headerOrdering
	 * @return
	 */
	public abstract List<Object[]> getBulkDataRow(String rScript, String[] headerOrdering);
	
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////

	/**
	 * This method is used to set the memory limit of R
	 */
	protected void setMemoryLimit() {
		String script = "memory.limit(" + RJavaTranslatorFactory.rMemory + ");";
		this.executeEmptyRDirect(script);
	}
	
	@Override
	public void initREnv(String env) {
		this.env = env;
		initREnv();
	}

	@Override
	public void initREnv() {
		this.executeEmptyRDirect("if(!exists(\"" + this.env + "\")) {" + this.env  + " <- new.env();}");
	}

	protected void removeRFunctions() {
		//adding all the calls i want removed from R. GG hackers. 
		String rScript="getenv <- function() {};Sys.chmod<-getenv;Sys.date<-getenv;Sys.getenv<-getenv;Sys.getlocate<-getenv;"
				+ "Sys.getpid<-getenv;Sys.glob<-getenv;Sys.info<-getenv;Sys.localeconv<-getenv;"
				+ "sys.on.exit<-getenv;sys.parent<-getenv;Sys.readlink<-getenv;Sys.setenv<-getenv;"
				+ "Sys.setlocale<-getenv;Sys.sleep<-getenv;sys.source<-getenv;sys.status<-getenv;"
				+ "Sys.timezone<-getenv;Sys.umask<-getenv;Sys.unsetenv<-getenv;"
				+ "Sys.which<-getenv;Sys<-getenv;sys<-getenv;install<-getenv;install.packages<-getenv;";
		// Sys.time<-getenv; <- used in TimestampDataReactor
		this.runR(rScript);
	}
	
	protected String encapsulateForEnv(String rScript) {
		//return rScript;
		String newRScript = "with(" + this.env + ", {" + rScript + "});";
		return newRScript;
	}
	
	/**
	 * Remove the existing R environment
	 * This should only be called when you drop the entire insight
	 */
	public void removeEnv() {
		executeEmptyRDirect("rm(" + this.env + ");");
		executeEmptyRDirect("gc()");
	}
	
	/**
	 * Function to run a script from one environment and assign the output into another environment
	 * @param trans1
	 * @param assignVar
	 * @param trans2
	 * @param rScript
	 */
	public static void loadDataBetweenEnv(AbstractRJavaTranslator trans1, String assignVar, AbstractRJavaTranslator trans2, String rScript) {
		String combinedScript = trans1.encapsulateForEnv( assignVar + "<- " + trans2.encapsulateForEnv(rScript) );
		trans1.executeEmptyR(combinedScript);
	}
	
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	
	/**
	 * This method is used to get the column names of a frame
	 * 
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		String script = "names(" + frameName + ");";
		String[] colNames = this.getStringArray(script);
		return colNames;
	}

	/**
	 * This method is used to get the column types of a frame
	 * 
	 * @param frameName
	 */
	// THIS METHOD IS OVERRIDEN IN BOTH SUBCLASSES
	// FOR JRI + RSERVE
	// DUE TO TYPES BEING AN ARRAY WHEN ORDERED FACTORS
	public String[] getColumnTypes(String frameName) {
		String script = "sapply(" + frameName + ", class);";
		String[] colTypes = this.getStringArray(script);
		return colTypes;
	}
	
	/**
	 * Determine if a frame is empty / exists
	 * 
	 * @param frameName
	 * @return
	 */
	public boolean isEmpty(String frameName) {
		String script = "(!exists(\"" + frameName + "\") || (is.data.table(" + frameName + ") && nrow(" + frameName + ") == 0))";
		return this.getBoolean(script);
	}
	
	public boolean varExists(String varname) {
		String script = "exists(\"" + varname + "\")";
		return this.getBoolean(script);
	}
	
	
	/**
	 * Alter a column to a new type
	 * @param frameName
	 * @param columnName
	 * @param typeToConvert
	 */
	public void changeColumnType(String frameName, String columnName, SemossDataType typeToConvert) {
		String rScript = null;
		if(typeToConvert == SemossDataType.STRING) {
			rScript = RSyntaxHelper.alterColumnTypeToCharacter(frameName, columnName);
		} else if(typeToConvert == SemossDataType.INT) {
			rScript = RSyntaxHelper.alterColumnTypeToNumeric(frameName, columnName);
		} else if(typeToConvert == SemossDataType.DOUBLE) {
			rScript = RSyntaxHelper.alterColumnTypeToNumeric(frameName, columnName);
		} 
		else if(typeToConvert == SemossDataType.DATE) {
			rScript = RSyntaxHelper.alterColumnTypeToDate(frameName, null, columnName);
		} else if(typeToConvert == SemossDataType.TIMESTAMP) {
			rScript = RSyntaxHelper.alterColumnTypeToDateTime(frameName, null, columnName);
		}
		this.executeEmptyR(rScript);
	}
	
	/**
	 * Alter a column to a new type if it is different from the current type
	 * @param frameName
	 * @param columnName
	 * @param typeToConvert
	 * @param currentType
	 */
	public void changeColumnType(String frameName, String columnName, SemossDataType typeToConvert, SemossDataType currentType) {
		if(typeToConvert != currentType) {
			changeColumnType(frameName, columnName, typeToConvert);
		}
	}

	/**
	 * This method is used to get the column type for a single column of a frame
	 * 
	 * @param frameName
	 * @param column
	 */
	public String getColumnType(String frameName, String column) {
		String script = "sapply(" + frameName + "$" + column + ", class)";
		String colType = this.getString(script);
		return colType;
	}

	public void changeColumnType(RDataTable frame, String frameName, String colName, String newType, String dateFormat) {
		String script = null;
		if (newType.equalsIgnoreCase("string")) {
			script = frameName + " <- " + frameName + "[, " + colName + " := as.character(" + colName + ")]";
			this.executeEmptyR(script);
		} else if (newType.equalsIgnoreCase("factor")) {
			script = frameName + " <- " + frameName + "[, " + colName + " := as.factor(" + colName + ")]";
			this.executeEmptyR(script);
		} else if (newType.equalsIgnoreCase("number")) {
			script = frameName + " <- " + frameName + "[, " + colName + " := as.numeric(" + colName + ")]";
			this.executeEmptyR(script);
		} else if (newType.equalsIgnoreCase("date")) {
			// we have a different script to run if it is a str to date
			// conversion
			// or a date to new date format conversion
			String type = this.getColumnType(frameName, colName);
			String tempTable = Utility.getRandomString(6);
			if (type.equalsIgnoreCase("date")) {
				String formatString = ", format = '" + dateFormat + "'";
				script = tempTable + " <- format(" + frameName + "$" + colName + formatString + ")";
				this.executeEmptyR(script);
				script = frameName + "$" + colName + " <- " + "as.Date(" + tempTable + formatString + ")";
				this.executeEmptyR(script);
			} else {
				script = tempTable + " <- as.Date(" + frameName + "$" + colName + ", format='" + dateFormat + "')";
				this.executeEmptyR(script);
				script = frameName + "$" + colName + " <- " + tempTable;
				this.executeEmptyR(script);
			}
			// perform variable cleanup
			this.executeEmptyR("rm(" + tempTable + ");");
			this.executeEmptyR("gc();");
		}
		logger.info("Successfully changed data type for column = " + colName);
		frame.getMetaData().modifyDataTypeToProperty(frameName + "__" + colName, frameName, newType);
	}

	/**
	 * Get number of rows from an r script
	 * 
	 * @param frameName
	 * @return
	 */
	public int getNumRows(String frameName) {
		String script = "nrow(" + frameName + ")";
		int numRows = this.getInt(script);
		return numRows;
	}

	/**
	 * This method is used to set the insight
	 * 
	 * @param insight
	 */
	@Override
	public void setInsight(Insight insight) {
		this.insight = insight;
		this.env = "a" + Utility.makeAlphaNumeric(insight.getInsightId());
		//initREnv();
	}
	
	/**
	 * This method is used to get the insight
	 * 
	 */
	@Override
	public Insight getInsight() {
		return this.insight;
		//initREnv();
	}

	/**
	 * This method is used to set the logger
	 * 
	 * @param logger
	 */
	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	/**
	 * This method is used initialize an empty matrix with the appropriate
	 * number of rows and columns
	 * @param matrix
	 * @param numRows
	 * @param numColumns
	 */
	public void initEmptyMatrix(List<Object[]> matrix, int numRows, int numCols) {
		for(int i = 0; i < numRows; i++) {
			matrix.add(new Object[numCols]);
		}
	}
	
	/**
	 * This method is used generate a r data.table from a given query
	 * Returns the r variable name that references the created data.table
	 * @param frame
	 * @param qs
	 * @return rFrame name
	 */
	public String generateRDataTableVariable(ITableDataFrame frame, SelectQueryStruct qs) {
		String dfName = "f_" + Utility.getRandomString(10);

		// these stringbuilders will build the new r table and populate the
		// table with the instance values
		StringBuilder instanceValuesBuilder = new StringBuilder(); // this puts values into the frame we are constructing
				
		// build instance list
		List<Object[]> instanceList = new ArrayList<Object[]>();
		
		// use an iterator to get the instance values from the qs
		// we will use these instance values to construct a new r data frame
		IRawSelectWrapper it = null;
		try {
			it = frame.query(qs);
			while (it.hasNext()) {
				Object[] values = it.next().getRawValues();
				instanceList.add(values);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		// now that we have the instance values, we can use them to build a string to populate the table that we will make in r
		// colNameString will keep track of the columns we are creating in our new r data frame
		List<IQuerySelector> inputSelectors = qs.getSelectors();
		int numSelectors = inputSelectors.size();
		StringBuilder colNameSb = new StringBuilder();
		for (int i = 0; i < numSelectors; i++) {
			// use the column name without the frame name
			colNameSb.append(inputSelectors.get(i).getAlias());
			colNameSb.append("= character()").append(",");
			for (int j = 0; j < instanceList.size(); j++) {
				instanceValuesBuilder.append(dfName + "[" + (j + 1) + "," + (i + 1) + "]");
				instanceValuesBuilder.append("<-");
				if (instanceList.get(j) == null) {
					instanceValuesBuilder.append("\"" + "" + "\"");
				} else {
					// replace underscores with spaces in the instance data
					instanceValuesBuilder.append("\"" + instanceList.get(j)[i].toString().replaceAll("_", " ") + "\"");
				}
				instanceValuesBuilder.append(";");
			}
		}

		// colNameString format: Title= character(),Genre= character()
		String colNameString = colNameSb.substring(0, colNameSb.length()-1);
		// scriptSb format: frame<-data.frame(Title= character(), stringsAsFactors = FALSE); + the instances
		StringBuilder scriptSb = new StringBuilder(); // this builds the dataframe
		scriptSb.append(dfName).append("<-data.frame(").append(colNameString).append(", stringsAsFactors = FALSE);")
			.append(instanceValuesBuilder.toString());
		// run the total script
		this.runR(scriptSb.toString());
		return dfName;
	}

	/**
	 * Check if r packages are installed
	 * 
	 * @param packages
	 * @throws IllegalArgumentException
	 *             error if an r package is missing
	 */
	public void checkPackages(String[] packages) {
		String packageError = "";
		String rScript = "required_packages<- c('" + StringUtils.join(packages,"','") + "');"
				+ "required_packages[!(required_packages %in% rownames(installed.packages()))]";
		String[] missingPackages = this.getStringArray(rScript);
		runR("rm(required_packages)");
		
		if (missingPackages.length > 0) {
			packageError += StringUtils.join(missingPackages, "\n");
			String errorMessage = "\nMake sure you have all the following R libraries installed:\n" + packageError;
			throw new SemossPixelException(new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		/*
		 * Require actually loads the library if it exists
		 */
//		String packageError = "";
//		int[] confirmedPackages = this.getIntArray("which(as.logical(lapply(list('" + StringUtils.join(packages, "','")
//				+ "')" + ", require, character.only=TRUE))==F)");
//
//		if (confirmedPackages.length > 0) {
//			for (int i : confirmedPackages) {
//				int index = i - 1;
//				packageError += packages[index] + "\n";
//			}
//			String errorMessage = "\nMake sure you have all the following R libraries installed:\n" + packageError;
//			throw new SemossPixelException(new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
//		}
	}
	
	/**
	 * Check if r packages are installed
	 * 
	 * @param packages
	 * @param logger
	 *            log missing packages
	 * @return true/false if all packages are installed
	 */
    public boolean checkPackages(String[] packages, Logger logger) {
    	boolean installed = true;
    	String packageError = "";
    	int[] confirmedPackages = this.getIntArray("which(as.logical(lapply(list('" + StringUtils.join(packages,"','") + "')"
    			+ ", require, character.only=TRUE))==F)");
    	
    	if (confirmedPackages.length > 0) {
    		for (int i : confirmedPackages){
    			int index = i - 1;
    			packageError += packages[index] + "\n";
    			installed = false;
    		}
    		String errorMessage = "\nMake sure you have all the following R libraries installed:\n" + packageError;
    		logger.info(errorMessage);
    	}
		return installed;
    }


	@Override    
	public void runR(String script) {
		// Clean the script
		script = script.trim();
		
		// Get temp folder and file locations
		// also define a ROOT variable
		String removePathVariables = "";
		String insightRootAssignment = "";
		String appRootAssignment = "";
		String userRootAssignment = "";

		String insightRootPath = null;
		String appRootPath = null;
		String userRootPath = null;
		
		String rTemp = null;
		if(this.insight != null) {
			insightRootPath = this.insight.getInsightFolder().replace('\\', '/');
			insightRootAssignment = "ROOT <- '" + insightRootPath.replace("'", "\\'") + "';";
			removePathVariables = "ROOT";
			
			if(this.insight.isSavedInsight()) {
				appRootPath = this.insight.getAppFolder();
				appRootPath = appRootPath.replace('\\', '/');
				appRootAssignment = "APP_ROOT <- '" + appRootPath.replace("'", "\\'") + "';";
				removePathVariables += ", APP_ROOT";
			}
			try {
				// you only have this if you are logged in
				if(this.insight.getUser() != null && !this.insight.getUser().isAnonymous()) {
					userRootPath = AssetUtility.getAssetBasePath(this.insight, AssetUtility.USER_SPACE_KEY, false);
					userRootPath = userRootPath.replace('\\', '/');
					userRootAssignment = "USER_ROOT <- '" + userRootPath.replace("'", "\\'") + "';";
					removePathVariables += ", USER_ROOT";
				}
			} catch(Exception ignore) {
				// ignore
			}
			
			rTemp = insightRootPath + "/R/Temp/";
		} else {
			rTemp = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/R/Temp/").replace('\\', '/');
		}
		
		File rTempF = new File(Utility.normalizePath(rTemp));
		if(!rTempF.exists()) {
			rTempF.mkdirs();
		}
		
		String scriptPath = rTemp + Utility.getRandomString(12) + ".R";
		File scriptFile = new File(Utility.normalizePath(scriptPath));	
		
		// attempt to put it into environment
		script = encapsulateForEnv(insightRootAssignment + appRootAssignment 
				+ userRootAssignment + script);
//		logger.info("#### Rscript ####: " +scriptFile.getName() + ": " + script);

		try {
			FileUtils.writeStringToFile(scriptFile, script);
			this.executeEmptyRDirect("source(\"" + scriptPath + "\", local=TRUE)");
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			try {
				if(!removePathVariables.isEmpty()) {
					this.executeEmptyR("rm(" + removePathVariables + ");");
					this.executeEmptyR("gc();"); // Garbage collection
				}
			} catch (Exception e) {
				logger.warn("Unable to cleanup R.", e);
			}
			if(scriptFile != null && scriptFile.exists()) {
				scriptFile.delete();
			}
		}
	}

	@Override
	public String runRAndReturnOutput(String script) {
		return runRAndReturnOutput(script, null);
	}

	@Override
	public String runRAndReturnOutput(String script, Map appMap) {
		// Clean the script
		script = script.trim();
		
		// update script with try catch
		script = "tryCatch({" + script + "}, error=function(e){e$message})";
		
		// Get temp folder and file locations
		// also define a ROOT variable
		String removePathVariables = "";
		String insightRootAssignment = "";
		String appRootAssignment = "";
		String userRootAssignment = "";

		String insightRootPath = null;
		String appRootPath = null;
		String userRootPath = null;
		
		String rTemp = null;
		if(this.insight != null) {
			insightRootPath = this.insight.getInsightFolder().replace('\\', '/');
			insightRootAssignment = "ROOT <- '" + insightRootPath.replace("'", "\\'") + "';";
			removePathVariables = ", ROOT";
			
			if(this.insight.isSavedInsight()) {
				appRootPath = this.insight.getAppFolder();
				appRootPath = appRootPath.replace('\\', '/');
				appRootAssignment = "APP_ROOT <- '" + appRootPath.replace("'", "\\'") + "';";
				removePathVariables += ", APP_ROOT";
			}
			try {
				// you only have this if you are logged in
				if(this.insight.getUser() != null && !this.insight.getUser().isAnonymous()) {
					userRootPath = AssetUtility.getAssetBasePath(this.insight, AssetUtility.USER_SPACE_KEY, false);
					userRootPath = userRootPath.replace('\\', '/');
					userRootAssignment = "USER_ROOT <- '" + userRootPath.replace("'", "\\'") + "';";
					removePathVariables += ", USER_ROOT";
				}
			} catch(Exception ignore) {
				// ignore
			}
			
			rTemp = insightRootPath + "/R/Temp/";
		} else {
			rTemp = (DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/R/Temp/").replace('\\', '/');
		}

		logger.info("Executing file at " + rTemp);
		File rTempF = new File(Utility.normalizePath(rTemp));
		if(!rTempF.exists()) {
			rTempF.mkdirs();
		}
		
		String scriptPath = rTemp + Utility.getRandomString(12) + ".R";
		File scriptFile = new File(scriptPath);	
		String outputPath = rTemp + Utility.getRandomString(12) + ".txt";
		File outputFile = new File(outputPath);
		
		// initialize the environment if not already
		// Wrap the script with our capture logic
		String randomVariable = "con" + Utility.getRandomString(7);
		
		// get the custom var String
		String varFolderAssignment = "";
		if(appMap != null && appMap.containsKey("R_VAR_STRING"))
			varFolderAssignment = appMap.get("R_VAR_STRING").toString();

		// attempt to put it into environment
		script = randomVariable + "<- file(\"" + outputPath + "\"); " + 
				"sink(" + randomVariable + ", append=TRUE, type=c('output','message') ); " +
				encapsulateForEnv(insightRootAssignment + appRootAssignment + userRootAssignment + varFolderAssignment +  script) +
				"sink();";

		// Try writing the script to a file
		try {
			FileUtils.writeStringToFile(scriptFile, script);
			String finalScript = "print(source(\"" + scriptPath + "\", print.eval=TRUE, local=TRUE)); ";

			// Try running the script, which saves the output to a file
			 // TODO >>>timb: R - we really shouldn't be throwing runtime ex everywhere for R (later)
			RuntimeException error = null;
			try {
				this.executeEmptyRDirect(finalScript);
			} catch (RuntimeException e) {
				error = e; // Save the error so we can report it
			}
			
			// Finally, read the output and return, or throw the appropriate error
			try {
				if(!outputFile.exists()) {
					throw new IllegalArgumentException("Failed to run R script. No output could be generated");
				}
				
				String output = FileUtils.readFileToString(outputFile).trim();
				
				// clean up the output
				if(userRootPath != null && output.contains(userRootPath)) {
					output = output.replace(userRootPath, "$USER_IF");
				}
				if(appRootPath != null && output.contains(appRootPath)) {
					output = output.replace(appRootPath, "$APP_IF");
				}
				if(insightRootPath != null && output.contains(insightRootPath)) {
					output = output.replace(insightRootPath, "$IF");
				}
				if(varFolderAssignment != null &&  varFolderAssignment.length() > 0)
				{
					output = cleanCustomVar(output, appMap);
				}
				
				// Error cases
				if (output.startsWith("Error in")) {
					throw new IllegalArgumentException(cleanErrorOutput(output));
				} else if (error != null) {
					throw error;
				}
				
				// Successful case
				return output;
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				// If we have the detailed error, then throw it
				if (error != null) {
					throw error;
				}
				if(e.getMessage() != null) {
					throw e;
				}
				// otherwise throw a generic one
				throw new IllegalArgumentException("Failed to run R script.");
			} finally {
				// Cleanup
				try {
					this.executeEmptyR("sink();");
					this.executeEmptyR("rm(" + randomVariable + removePathVariables + ");");
					this.executeEmptyR("gc();"); // Garbage collection
				} catch (Exception e) {
					logger.warn("Unable to cleanup R.", e);
				}
				// delete the files
				if(outputFile != null && outputFile.exists()) {
					outputFile.delete();
				}
				if(scriptFile != null && scriptFile.exists()) {
					scriptFile.delete();
				}
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Error in writing R script for execution.", e);
		} finally {
			// Cleanup
			if(outputFile != null && outputFile.exists()) {
				outputFile.delete();
			}
			if(scriptFile != null && scriptFile.exists()) {
				scriptFile.delete();
			}
		}
	}
		
	private static String cleanErrorOutput(String output) {
		output = output.replaceAll("Error in eval\\(expr, envir, enclos\\) : ", "")
				 .replaceAll("Error in eval\\(ei, envir\\) : ", "")
				 .replaceAll("\r", "")
				 .replaceAll("\n", " ");
		int index = output.indexOf("In addition");
		if (index != -1) {
			output = output.substring(0, index);
		}
		return output.trim();
	}
	
	
	// make the custom var String
	private String cleanCustomVar(String output, Map <String, StringBuffer> appMap)
	{
		Iterator <String> varIt = appMap.keySet().iterator();

		while(varIt.hasNext())
		{
			// get this key
			String thisKey = varIt.next();
			String thisVal = appMap.get(thisKey).toString();
			
			output = output.replace(thisVal, thisKey);			
		}
		
		return output;
	}
	
}