package prerna.reactor.algorithms;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.impl.util.MetadataUtility;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;

public class NLPInstanceCacheReactor extends AbstractRFrameReactor {

	/**
	 * Reads in the Databases and Creates an RDS file of all unique instances in the AnalyticsRoutineScripts Folder
	 */

	// NLPInstanceCache(app=["2c8c41da-391a-4aa8-a170-9925211869c8"],table=[],columns=[],updateExistingValues=[false]);
	// NLPInstanceCache(app=["2c8c41da-391a-4aa8-a170-9925211869c8"],table=["Title"],columns=["Title"],updateExistingValues=[false]);

	private static final String UPDATE_EXISTING_VALUES = "updateExistingValues";
	protected static final String CLASS_NAME = NLPInstanceCacheReactor.class.getName();

	public NLPInstanceCacheReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.TABLE.getKey() , ReactorKeysEnum.COLUMNS.getKey(), UPDATE_EXISTING_VALUES };
	}

	@Override
	public NounMetadata execute() {
		// initialize inputs
		init();
		organizeKeys();
		String databaseId = UploadInputUtility.getDatabaseNameOrId(this.store);
		String table = this.keyValue.get(this.keysToGet[1]);
		List<String> columnsToUpdate = getSpecificColumns(databaseId, table);
		boolean updateExistingValues = updateExistingValues();
		boolean allValues = false;
		String gc = "";
		
		// if no columns were entered, default to all string columns
		// currently have to reuse input in the case that user accidentally
		// inputs only non-string columns
		if(this.store.getNoun(this.keysToGet[2]) == null || this.store.getNoun(this.keysToGet[2]).isEmpty()) {
			allValues = true;
		}
		
		// check for rds file
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder").replace("\\", "/");
		String filePath = baseFolder + "/R/AnalyticsRoutineScripts/unique-values-table.rds";
		File rdsFile = new File(filePath);
		String rdsFrame = "rdsFrame_" + Utility.getRandomString(5);
		String rdsFrameTrim = "rdsFrame_" + Utility.getRandomString(5);
		boolean rdsExists = rdsFile.exists();

		// keep the existing cols and tables lists to skip 
		// through later when not updating values
		List<String> existingTableCols = new Vector<String>();
		
		// if the rds exists, the filter out the existing cols from colFilters if
		// desired
		if (rdsExists) {
			// creates the rds table to rbind to later
			this.rJavaTranslator.runR(rdsFrame + " <- readRDS(\"" + filePath + "\");");

			// if updating existing, then remove the current values
			// otherwise, skip the duplicate columns to improve performance
			if (!updateExistingValues) {
				if(allValues) {
					// in this case, we need to store all the cols/tables that we already have
					// then we will filter these out when adding cols
					
					// get the unique columns and tables into arrays
					String uniqueColsTables = rdsFrameTrim + " <- unique(" + rdsFrame + "[c(\"Table\", \"Column\", \"AppID\")])"; 
					this.rJavaTranslator.runR(uniqueColsTables);
					String existingColsScript = rdsFrameTrim + "[" + rdsFrameTrim + "$AppID == \"" + databaseId + "\",]$Column;";
					String existingTablesScript = rdsFrameTrim + "[" + rdsFrameTrim + "$AppID == \"" + databaseId + "\",]$Table;";
					String[] existingCols = this.rJavaTranslator.getStringArray(existingColsScript);
					String[] existingTables = this.rJavaTranslator.getStringArray(existingTablesScript);
					
					// store all the cols/tables that we already have
					for(int x = 0; x < existingCols.length ; x++) {
						String existingTable = existingTables[x].toString();
						String existingCol = existingCols[x].toString();
						if(existingTableCols == null || !existingTableCols.contains(existingTable+existingCol)) {
							existingTableCols.add(existingTable+existingCol+"");
						}
					}	
				} else {
					// in this case, we know the table of the columns and columnsToUpdate is not null
					// so just remove those columns that match by app id, table, and column
					String existingColsScript = "unique(" + rdsFrame + "[" + rdsFrame + "$AppID == \"" + databaseId + "\" & " + rdsFrame + "$Table == \"" + table + "\" ,]$Column);";
					String[] existingCols = this.rJavaTranslator.getStringArray(existingColsScript);
					columnsToUpdate.removeAll(Arrays.asList(existingCols));
				}
			} else {
				String removeScript = "";
				if(allValues) {
					// in this case, we can just delete all values for this db
					removeScript = rdsFrame + " <- " + rdsFrame + "[( " + "!( " + rdsFrame + "$AppID==\"" + databaseId + "\")),];";
				} else {
					// in this case, we need to delete based on cols and tables provided
					// Assumption: The User used the UI and, therefore, only one table is possible
					removeScript = rdsFrame + " <- " + rdsFrame + "[( ";
					String amp = "";
					for (String col : columnsToUpdate ) {
						removeScript += amp;
						amp = " & ";
						removeScript += "!("+rdsFrame+"$AppID==\""+databaseId+"\" & " + rdsFrame +"$Column==\"" + col + "\" & " + rdsFrame +"$Table==\"" + table + "\")";
					}
					removeScript += " ),];";
				}
				this.rJavaTranslator.runR(removeScript);
			}
		}
		
		if((columnsToUpdate == null || columnsToUpdate.isEmpty()) && !allValues) {
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
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		boolean ignoreData = MetadataUtility.ignoreConceptData(database.getDatabaseType());
		
		// loop through each column with in the app
		if(allValues) {
			int counter = 0;
			List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(databaseId);
			for(Object[] tableCol : allTableCols) {
				// only care about string values
				if(tableCol[2] == null || !tableCol[2].equals(SemossDataType.STRING.toString())) {
					continue;
				}
				
				// get the values
				String parent = tableCol[0].toString();
				String name = tableCol[1].toString();
				Boolean pk = (boolean) tableCol[3];
				String colQs = null;
				if(pk) {
					parent = name;
					colQs = name;
				} else {
					colQs = parent + "__" + name;
				}
				
				// drop columns we already have if not updating values
				if(!updateExistingValues && existingTableCols.contains(parent+name)) {
					continue;
				}
				
				// add the columns in batches of 20
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
				
				processSingleColumn(database, colQs, databaseId, appIdVector, tableVector, colVector, valueVector);
			}
			
			// process anything left over
			if(counter > 0) {
				addTable(retFrame, tempFrame, appIdVector, tableVector, colVector, valueVector);
			}
		} else {
			if(!ignoreData) {
				// we have a RDF/Graph database where we need to index the concept itself
				
				String colQs = table;
				processSingleColumn(database, colQs, databaseId, appIdVector, tableVector, colVector, valueVector);
			}
			
			// loop through the passed in columns
			for (String col : columnsToUpdate) {
				// now add all the other columns
				String colQs = table + "__" + col;
				processSingleColumn(database, colQs, databaseId, appIdVector, tableVector, colVector, valueVector);
			}
			
			// add table at the end since we only do 1 table when specifying columns
			addTable(retFrame, tempFrame, appIdVector, tableVector, colVector, valueVector);
		}

		// create gc string
		gc += retFrame + "," + tempFrame;
		
		// now save it to an RDS Files
		StringBuilder saveBuilder = new StringBuilder();
		if (rdsExists) {
			gc += " , " + rdsFrame + rdsFrameTrim;
			saveBuilder.append(retFrame + " <- rbind(" + retFrame + "," + rdsFrame + ");");
		}
		saveBuilder.append("saveRDS(" + retFrame + ", file = \"unique-values-table.rds\");");

		// reset wd and run
		saveBuilder.append("setwd(origDir);");
		this.rJavaTranslator.runR(saveBuilder.toString());

		// garbage cleanup
		this.rJavaTranslator.executeEmptyR("rm( " + gc + " , origDir ); gc();");
		System.out.println("");

		// return true
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully cached instances for NLP searching", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
	private void processSingleColumn(IDatabaseEngine database,
			String colQs, 
			String databaseId,
			List<String> databaseIdVector, 
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
			iterator = WrapperManager.getInstance().getRawWrapper(database, qs);
		} catch (Exception e) {
			return;
		}

		// all values are null in this column so continue
		if (!iterator.hasNext()) {
			return;
		}

		// for each value, create a row in our data table (i.e. add to the vector)
		while (iterator.hasNext()) {
			databaseIdVector.add(databaseId);
			if(colQs.contains("__")) {
				String[] split = colQs.split("__");
				tableVector.add(split[0]);
				colVector.add(split[1]);
			} else {
				tableVector.add(colQs);
				colVector.add(colQs);
			}

			// only add the first 50 characters of a value
			String value = iterator.next().getValues()[0].toString();
			if (value.length() > 50) {
				value = value.substring(0, 49);
			}
			
			// clean up some strings to work with R syntax
			if(value.contains("\\")) {
				value = value.replace("\\", "/");
			}
			
			if(value.contains("\"")) {
				value = value.replace("\"", "\\\"");
			} 
			
			valueVector.add(value);
		}
	}
	
	/**
	 * Process the table
	 * @param rdsFrame
	 * @param tempFrame
	 * @param databaseIdVector
	 * @param tableVector
	 * @param colVector
	 * @param valueVector
	 */
	private void addTable(String rdsFrame, String tempFrame, List<String> databaseIdVector, List<String> tableVector,
			List<String> colVector, List<String> valueVector) {
		// run this one separately to avoid the string getting too long
		String valueVectorName = "valueVector_" + Utility.getRandomString(5);
		String vectorScript = RSyntaxHelper.createStringRColVec(valueVector);
		String gc = valueVectorName;

		// just add the new vector name and run it
		vectorScript = valueVectorName + " <- " + vectorScript + ";";
		this.rJavaTranslator.runR(vectorScript);

		// rbind to current frame
		String addTableScript = tempFrame + " <- data.frame(AppID = " + RSyntaxHelper.createStringRColVec(databaseIdVector)
				+ " , Table = " + RSyntaxHelper.createStringRColVec(tableVector) + " , Column = "
				+ RSyntaxHelper.createStringRColVec(colVector) + " , Value = " + valueVectorName
				+ ", stringsAsFactors = FALSE);" + rdsFrame + " <- rbind(" + rdsFrame + "," + tempFrame + ");";
		this.rJavaTranslator.runR(addTableScript);

		// garbage cleanup
		this.rJavaTranslator.executeEmptyR("rm( " + gc + ");");
	}
	
	/**
	 * Get specific string columns in the table - make sure they are strings
	 * @param databaseId
	 * @param table
	 * @return
	 */
	private List<String> getSpecificColumns(String databaseId, String table) {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(this.keysToGet[2]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<String>();
				for (Object obj : values) {
					String objType = "";
					if(table.equals(obj.toString())) {
						objType = MasterDatabaseUtility.getBasicDataType(databaseId, obj.toString(), null);
					} else {
						objType = MasterDatabaseUtility.getBasicDataType(databaseId, obj.toString(), table);
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
