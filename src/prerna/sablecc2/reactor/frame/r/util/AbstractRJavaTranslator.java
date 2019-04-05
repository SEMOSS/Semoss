package prerna.sablecc2.reactor.frame.r.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractRJavaTranslator implements IRJavaTranslator {

	Insight insight = null;
	Logger logger = null;
	String env = "default";
	
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////

	/*
	 * Abstract methods defined here
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
		this.runR(script);
		
	}
	
	protected String encapsulateForEnv(String rScript) {
		return rScript;
//		String newRScript = "with(" + this.env + ", {" + rScript + "});";
//		return newRScript;
	}
	
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
		String script = "sapply(" + frameName + "$" + column + ", class);";
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

		// use an iterator to get the instance values from the qs
		// we will use these instance values to construct a new r data frame
		Iterator<IHeadersDataRow> it = (Iterator<IHeadersDataRow>) frame.query(qs);
		
		// these stringbuilders will build the new r table and populate the
		// table with the instance values
		StringBuilder instanceValuesBuilder = new StringBuilder(); // this puts values into the frame we are constructing

		// build instance list
		List<Object[]> instanceList = new ArrayList<Object[]>();
		while (it.hasNext()) {
			Object[] values = it.next().getRawValues();
			instanceList.add(values);
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
		int[] confirmedPackages = this.getIntArray("which(as.logical(lapply(list('" + StringUtils.join(packages, "','")
				+ "')" + ", require, character.only=TRUE))==F)");

		if (confirmedPackages.length > 0) {
			for (int i : confirmedPackages) {
				int index = i - 1;
				packageError += packages[index] + "\n";
			}
			String errorMessage = "\nMake sure you have all the following R libraries installed:\n" + packageError;
			throw new SemossPixelException(new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
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
		String insightCacheLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
		String csvInsightCacheFolder = DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		String baseDir = insightCacheLoc + "\\" + csvInsightCacheFolder + "\\";
		String tempFileLocation = baseDir + Utility.getRandomString(15) + ".R";
		tempFileLocation = tempFileLocation.replace("\\", "/");

		File f = new File(tempFileLocation);
		try {
			FileUtils.writeStringToFile(f, script);
		} catch (IOException e1) {
			System.out.println("Error in writing R script for execution!");
			e1.printStackTrace();
		}

		try {
			this.executeEmptyR("source(\"" + tempFileLocation + "\", local=TRUE)");
		} finally {
			f.delete();
		}
	}

	@Override
	public String runRAndReturnOutput(String script) {
		// TODO >>>timb: R - Refactor this stuff and pull in error properly (later)
		String insightCacheLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
		String csvInsightCacheFolder = DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		String baseDir = insightCacheLoc + "\\" + csvInsightCacheFolder + "\\";
		String tempFileLocation = baseDir + Utility.getRandomString(15) + ".R";
		tempFileLocation = tempFileLocation.replace("\\", "/");

		String outputLoc = baseDir + Utility.getRandomString(15) + ".txt";
		outputLoc = outputLoc.replace("\\", "/");
		File outputF = new File(outputLoc);
		try {
			outputF.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String randomVariable = "con" + Utility.getRandomString(6);
		File f = new File(tempFileLocation);
		try {
			script = script.trim();
			if(!script.endsWith(";")) {
				script = script +";";
			}
			script = randomVariable + "<- file(\"" + outputLoc + "\"); sink(" + randomVariable + ", append=TRUE, type=\"output\"); "
					+ "sink(" + randomVariable + ", append=TRUE, type=\"message\"); " + script + " sink();";
			FileUtils.writeStringToFile(f, script);
		} catch (IOException e1) {
			System.out.println("Error in writing R script for execution!");
			e1.printStackTrace();
		}

		String scriptOutput = null;
		try {
			String finalScript = "print(source(\"" + tempFileLocation + "\", print.eval=TRUE, local=TRUE)); ";
			this.executeR(finalScript);
			try {
				scriptOutput = FileUtils.readFileToString(outputF);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} finally {
			f.delete();
			outputF.delete();
		}
		
		// drop the random con variable
		this.executeEmptyR("rm(" + randomVariable + ")");
		this.executeEmptyR("gc()");
		
		// return the final output
		return scriptOutput.trim();
	}
}
