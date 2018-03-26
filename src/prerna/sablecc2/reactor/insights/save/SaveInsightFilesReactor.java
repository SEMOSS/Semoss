package prerna.sablecc2.reactor.insights.save;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IEngine;
import prerna.poi.main.InsightFilesToDatabaseReader;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.imports.FileMeta;
import prerna.util.Utility;

public class SaveInsightFilesReactor extends AbstractReactor {

	private static final String CLASS_NAME = SaveInsightFilesReactor.class.getName();

	public SaveInsightFilesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), "originalFile"};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();

		String appName = this.keyValue.get(this.keysToGet[0]);
		if(appName == null || appName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the app name");
		}

		// the engine we are creating
		IEngine createdEng = null;
		Set<String> newTables = null;
		
		ITableDataFrame dataframe = (ITableDataFrame) this.insight.getDataMaker();
		if(dataframe instanceof H2Frame) {
			createdEng = createEngine( (H2Frame) dataframe, appName);
		} else if(dataframe instanceof RDataTable) {
			createdEng = createEngine( (RDataTable) dataframe, appName);
		} else {
			InsightFilesToDatabaseReader creator = new InsightFilesToDatabaseReader();
			try { 
				createdEng = creator.processInsightFiles(this.insight, appName);
				newTables = creator.getNewTables();
			} catch (IOException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Error saving data as an app!");
			}
		}
		logger.info("Done loading files in insight into database");

		Map<String, List<String>> newTablesAndCols = new Hashtable<String, List<String>>();
		for(String newTable : newTables) {
			List<String> props = createdEng.getProperties4Concept(newTable, true);
			newTablesAndCols.put(newTable, props);
		}
		
		Map<String, Object> retData = updateFromFileToApp(appName, this.insight.getFilesUsedInInsight(), newTablesAndCols, logger);
		return new NounMetadata(retData, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	/**
	 * Update the recipe to use the new engine
	 * @param appName
	 * @param filesMetadata
	 * @param newTablesAndCols
	 * @param logger
	 * @return
	 */
	private Map<String, Object> updateFromFileToApp(String appName, List<FileMeta> filesMetadata, Map<String, List<String>> newTablesAndCols, Logger logger) {
		// this will be used to keep track of the old parent to the new parent
		// this is needed so the FE can properly create parameters
		Map<String, String> parentMap = new HashMap<String, String>();

		// need to update the recipe now
		for(FileMeta fileMeta : filesMetadata) {

			// this is the pixel string we need to update
			String pixelStringToFind = fileMeta.getOriginalFile();
			List<String> listPixelStrings = this.insight.getPixelRecipe();

			// keep track of all statements
			// only used if updatePkqlExpression boolean becomes true
			List<String> newPixelRecipe = new Vector<String>();

			// this is the list of headers that were uploaded into the frame
			List<IQuerySelector> selectorsToMatch = fileMeta.getSelectors();

			// need to iterate through and update the correct pkql
			for(int pkqlIdx = 0; pkqlIdx < listPixelStrings.size(); pkqlIdx++) {
				String pkslExpr = listPixelStrings.get(pkqlIdx);
				// we store the API Reactor string
				// but this will definitely be stored within a data.import
				if(pkslExpr.contains(pixelStringToFind)) {

					// find the new table that was created from this file
					String tableToUse = null;
					TABLE_LOOP : for(String newTable : newTablesAndCols.keySet()) {
						// get the list of columns for the table that exists in the engine
						List<String> selectors = newTablesAndCols.get(newTable);

						// need to see if all selectors match
						SELECTOR_MATCH_LOOP : for(IQuerySelector selectorInFile : selectorsToMatch) {
							boolean selectorFound = false;
							if(selectorInFile.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
								for(String selectorInTable : selectors) {
									// we found a match, we are good
									// format of selector in table is http://semoss.org/ontologies/Relation/Contains/Rotten_Tomatoes_Audience/MOVIECSV
									QueryColumnSelector cQuerySelector = (QueryColumnSelector) selectorInFile;
									String compare = cQuerySelector.getColumn();
									if(compare.equalsIgnoreCase(Utility.getClassName(selectorInTable))) {
										selectorFound = true;
										continue SELECTOR_MATCH_LOOP;
									}
								}
							}
							if(selectorFound == false) {
								// if we hit this point, then there was a selector
								// in selectorsToMatch that wasn't found in the tableSelectors
								// lets look at next table
								continue TABLE_LOOP;
							}
						} // end SELECTOR_MATCH_LOOP

						// if we hit this point, then everything matched!
						tableToUse = newTable;
						break TABLE_LOOP;
					}
					// this will update the pkql query to run
					newPixelRecipe.add(fileMeta.generatePixelOnEngine(appName, tableToUse));
					
				} else {
					newPixelRecipe.add(pkslExpr);
				}
			}
			this.insight.setPixelRecipe(newPixelRecipe);
		}


		logger.info("Done modifying PKQL to query of new engine");

		// clear the files since they are now loaded into the engine
		filesMetadata.clear();

		// we will return the new insight recipe after the PKQL has been modified
		Map<String, Object> retData = new HashMap<String, Object>();

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		List<String> pkqlRecipe = this.insight.getPixelRecipe();
		for(String command: pkqlRecipe) {
			Map<String, Object> retMap = new HashMap<String, Object>();
			retMap.put("command", command);
			list.add(retMap);
		}

		retData.put("parentMap", parentMap);
		retData.put("recipe", list);
		
		return retData;
	}

	
	//////////////////////////////////////////////////////////////
	
	/*
	 * Make a new engine from a csv file
	 */
	
	private IEngine createEngine(RDataTable dataframe, String appName) {
		return null;
	}

	private IEngine createEngine(H2Frame dataframe, String appName) {
		return null;
	}


}
