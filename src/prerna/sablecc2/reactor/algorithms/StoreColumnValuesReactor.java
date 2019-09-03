package prerna.sablecc2.reactor.algorithms;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.impl.util.MetadataUtility;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

//TODO: change to NLPInstanceCacheReactor
public class StoreColumnValuesReactor extends AbstractRFrameReactor {

	/**
	 * Reads in the Databases and Creates an RDS file of all unique instances in the AnalyticsRoutineScripts Folder
	 */

	// StoreColumnValues(app=["2c8c41da-391a-4aa8-a170-9925211869c8"],table=[],columns=[],addDuplicates=[false]);
	// StoreColumnValues(app=["2c8c41da-391a-4aa8-a170-9925211869c8"],table=["Title"],columns=["Title"],addDuplicates=[false]);

	// TODO: change addDuplicates to updateExistingValues
	private static final String ADD_DUPLICATES = "addDuplicates";
	protected static final String CLASS_NAME = StoreColumnValuesReactor.class.getName();

	public StoreColumnValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.TABLE.getKey() , ReactorKeysEnum.COLUMNS.getKey(), ADD_DUPLICATES };
	}

	@Override
	public NounMetadata execute() {
		// initialize inputs
		init();
		organizeKeys();
		String appId = UploadInputUtility.getAppName(this.store);
		String table = this.keyValue.get(this.keysToGet[1]);
		List<String> columnsToUpdate = getSpecificColumns(appId, table);
		boolean updateExistingValues = updateExistingValues();
		boolean allValues = false;
		
		// if no columns were entered, default to all string columns
		if(columnsToUpdate == null || columnsToUpdate.isEmpty()) {
			allValues = true;
		}
		
		// check for rds file
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder").replace("\\", "/");
		String filePath = baseFolder + "/R/AnalyticsRoutineScripts/unique-values-table.rds";
		File rdsFile = new File(filePath);
		String rdsFrame = "rdsFrame_" + Utility.getRandomString(5);
		boolean rdsExists = rdsFile.exists();

		//TODO: need to fix the rdsExist
		//TODO: need to delete with table name
		//TODO: if you are allValues, then delete just based on appID
		
		// if the rds exists, the filter out the existing cols from colFilters if
		// desired
		if (rdsExists) {
			// creates the rds table to rbind to later
			this.rJavaTranslator.runR(rdsFrame + " <- readRDS(\"" + filePath + "\");");

			// if updating existing, then remove the current values
			// otherwise, skip the duplicate columns to improve performance
			if (!updateExistingValues) {
				String existingColsScript = "unique(" + rdsFrame + "[" + rdsFrame + "$AppID == \"" + appId + "\",]$Column);";
				String[] existingCols = this.rJavaTranslator.getStringArray(existingColsScript);
				columnsToUpdate.removeAll(Arrays.asList(existingCols));
			} else {
				// get the current columns that we have in that app
				String existingColsScript = "unique(" + rdsFrame + "[" + rdsFrame + "$AppID == \"" + appId + "\",]$Column);";
				List<String> existingCols = Arrays.asList(this.rJavaTranslator.getStringArray(existingColsScript));
				
				// get the intersection of existing columns and columns inputted
				ArrayList<String> colsToRemove = new ArrayList<String>(existingCols);
				colsToRemove.retainAll(columnsToUpdate);
				
				// now remove the current ones that we are about to replace with this script
				String removeScript = rdsFrame + " <- " + rdsFrame + "[( ";
				String amp = "";
				for (String col : colsToRemove ) {
					removeScript += amp;
					amp = " & ";
					removeScript += "!("+rdsFrame+"$AppID==\""+appId+"\" & "+rdsFrame+"$Column==\""+col+"\")";
				}
				removeScript += " ),];";
				this.rJavaTranslator.runR(removeScript);
			}
		}
		
		if(columnsToUpdate.isEmpty()) {
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		}

		// store and change working directory
		String retFrame = "retFrame_" + Utility.getRandomString(5);

		StringBuilder setupBuilder = new StringBuilder();
		setupBuilder.append("origDir <- getwd();");
		setupBuilder.append("setwd(\"" + baseFolder.replace("\\", "/") + "/R/AnalyticsRoutineScripts/\");");
		setupBuilder.append(retFrame + " <- data.frame(AppID = character(), Table = character(), Column = character(), Value = character() , stringsAsFactors = FALSE);");
		this.rJavaTranslator.runR(setupBuilder.toString());

		String tempFrame = "tempFrame_" + Utility.getRandomString(5);
		// create empty vectors that will populate the temp dfs
		List<String> appIdVector = new ArrayList<String>();
		List<String> tableVector = new ArrayList<String>();
		List<String> colVector = new ArrayList<String>();
		List<String> valueVector = new ArrayList<String>();

		// a concept (or table) in RDBMS/R has no meaning - the data is in the properties (columns)
		IEngine engine = Utility.getEngine(appId);
		boolean ignoreData = MetadataUtility.ignoreConceptData(engine.getEngineType());
		
		// loop through each column with in the app
		if(allValues) {
			int counter = 0;
			List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(appId);
			for(Object[] tableCol : allTableCols) {
				// only care about string values
				if(!tableCol[2].equals(SemossDataType.STRING.toString())) {
					continue;
				}
				String parent = tableCol[0].toString();
				String name = tableCol[1].toString();
				String colQs = null;
				if(parent == null) {
					colQs = name;
				} else {
					colQs = parent + "__" + name;
				}
				
				counter++;
				if(counter % 20 == 0) {
					// add the info
					addTable(retFrame, tempFrame, appIdVector, tableVector, colVector, valueVector);
					
					// reset everything to start process over
					appIdVector.clear();
					tableVector.clear();
					colVector.clear();
					valueVector.clear();
					counter = 0;
				}
				
				processSingleColumn(engine, colQs, appId, appIdVector, tableVector, colVector, valueVector);
			}
			
			// process anything left over
			if(counter > 0) {
				addTable(retFrame, tempFrame, appIdVector, tableVector, colVector, valueVector);
			}
		} else {
			if(!ignoreData) {
				// we have a RDF/Graph database where we need to index the concept itself
				
				String colQs = table;
				processSingleColumn(engine, colQs, appId, appIdVector, tableVector, colVector, valueVector);
			}
			
			// loop through the passed in columns
			for (String col : columnsToUpdate) {
				// now add all the other columns
				String colQs = table + "__" + col;
				processSingleColumn(engine, colQs, appId, appIdVector, tableVector, colVector, valueVector);
			}
			
			// add table at the end since we only do 1 table when specifying columns
			addTable(retFrame, tempFrame, appIdVector, tableVector, colVector, valueVector);
		}

		// create gc string
		String gc = retFrame + "," + tempFrame;
		// now save it to an RDS Files
		StringBuilder saveBuilder = new StringBuilder();
		if (rdsExists) {
			gc += " , " + rdsFrame;
			saveBuilder.append(retFrame + " <- rbind(" + retFrame + "," + rdsFrame + ");");
		}
		saveBuilder.append("saveRDS(" + retFrame + ", file = \"unique-values-table.rds\");");

		// reset wd and run
		saveBuilder.append("setwd(origDir);");
		this.rJavaTranslator.runR(saveBuilder.toString());

		// garbage cleanup
		this.rJavaTranslator.executeEmptyR("rm( " + gc + " , origDir ); gc();");

		// return true
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully indexed data for NLP searching", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
	private void processSingleColumn(IEngine engine,
			String colQs, 
			String appId,
			List<String> appIdVector, 
			List<String> tableVector,
			List<String> colVector, 
			List<String> valueVector) {
		// init variables for the col
		SelectQueryStruct qs = new SelectQueryStruct();
		
		QueryColumnSelector colSelector = new QueryColumnSelector(colQs);
		qs.addSelector(colSelector);

		// select only non-null values from database
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(colSelector, "!=", null, PixelDataType.NULL_VALUE));

		// select distinct values
		qs.setDistinct(true);

		// get the data for the db
		IRawSelectWrapper iterator = null;
		try {
			iterator = WrapperManager.getInstance().getRawWrapper(engine, qs);
		} catch (Exception e) {
			return;
		}

		// all values are null in this column so continue
		if (!iterator.hasNext()) {
			return;
		}

		// for each value, create a row in our data table (i.e. add to the vector)
		while (iterator.hasNext()) {
			appIdVector.add(appId);
			if(colQs.contains("__")) {
				String[] split = colQs.split("__");
				tableVector.add(split[0]);
				tableVector.add(split[1]);
			} else {
				tableVector.add(colQs);
				colVector.add(colQs);
			}

			// only add the first 50 characters of a value
			String value = iterator.next().getValues()[0].toString();
			if (value.length() > 50) {
				value = value.substring(0, 49);
			}
			valueVector.add(value);
		}
	}
	
	/**
	 * Process the table
	 * @param rdsFrame
	 * @param tempFrame
	 * @param appIdVector
	 * @param tableVector
	 * @param colVector
	 * @param valueVector
	 */
	private void addTable(String rdsFrame, String tempFrame, List<String> appIdVector, List<String> tableVector,
			List<String> colVector, List<String> valueVector) {
		// run this one separately to avoid the string getting too long
		String valueVectorName = "valueVector_" + Utility.getRandomString(5);
		String vectorScript = RSyntaxHelper.createStringRColVec(valueVector);
		String gc = valueVectorName;

		// if the values vector is too large, break it into doable pieces
		if (vectorScript.length() > 200000) {
			// set up two vectors to run
			String valueVectorName1 = "valueVector1_" + Utility.getRandomString(5);
			String valueVectorName2 = "valueVector2_" + Utility.getRandomString(5);

			// find that midpoint comma
			int index = vectorScript.indexOf("\",\"", vectorScript.length() / 2);

			// create the first half of the vector
			this.rJavaTranslator.runR(valueVectorName1 + " <- " + vectorScript.substring(0, index) + "\");");

			// create the second half of the vector
			this.rJavaTranslator.runR(valueVectorName2 + " <- c(" + vectorScript.substring(index + 2, vectorScript.length()) + ";");

			// merge them together
			this.rJavaTranslator.runR(valueVectorName + " <- c(" + valueVectorName1 + "," + valueVectorName2 + ");");

			// add those two to the gc
			gc += "," + valueVectorName1 + "," + valueVectorName2;
		} else {
			// just add the new vector name and run it
			vectorScript = valueVectorName + " <- " + vectorScript + ";";
			this.rJavaTranslator.runR(vectorScript);
		}

		// rbind to current frame
		String addTableScript = tempFrame + " <- data.frame(AppID = " + RSyntaxHelper.createStringRColVec(appIdVector)
				+ " , Table = " + RSyntaxHelper.createStringRColVec(tableVector) + " , Column = "
				+ RSyntaxHelper.createStringRColVec(colVector) + " , Value = " + valueVectorName
				+ ", stringsAsFactors = FALSE);" + rdsFrame + " <- rbind(" + rdsFrame + "," + tempFrame + ");";
		this.rJavaTranslator.runR(addTableScript);

		// garbage cleanup
		this.rJavaTranslator.executeEmptyR("rm( " + gc + ");");
	}
	
	/**
	 * Get specific string columns in the table - make sure they are strings
	 * @param appId
	 * @param table
	 * @return
	 */
	private List<String> getSpecificColumns(String appId, String table) {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(this.keysToGet[2]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<String>();
				for (Object obj : values) {
					String objType = "";
					if(table.equals(obj.toString())) {
						objType = MasterDatabaseUtility.getBasicDataType(appId, obj.toString(), null);
					} else {
						objType = MasterDatabaseUtility.getBasicDataType(appId, obj.toString(), table);
					}
					
					if(objType.equals(SemossDataType.STRING.toString())) {
						strValues.add(obj.toString());
					}
				}
				return strValues;
			}
		}
		return null;
	}
	
	/**
	 * Determine if we should override existing values
	 * @return
	 */
	private boolean updateExistingValues() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[3]);
		if (grs == null || grs.isEmpty()) {
			return false;
		}
		return (boolean) grs.get(0);
	}
}
