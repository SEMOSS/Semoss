package prerna.reactor.database.metaeditor.relationships;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.reactor.frame.r.util.IRJavaTranslator;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class AddBulkOwlRelationshipsReactor extends AbstractMetaEditorReactor {

	private static final Logger classLogger = LogManager.getLogger(AddBulkOwlRelationshipsReactor.class);

	private static final String CLASS_NAME = AddBulkOwlRelationshipsReactor.class.getName();
	private static final String PROP_MAX = "propagation";
	
	/*
	 * This class assumes that the start table, start column, end table, and end column have already been defined
	 */
	
	public AddBulkOwlRelationshipsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.FRAME.getKey(), PROP_MAX, STORE_VALUES_FRAME};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		databaseId = testDatabaseId(databaseId, true);

		String distanceStr = this.keyValue.get(this.keysToGet[2]);
		if(distanceStr == null || distanceStr.isEmpty()) {
			// default to direct matches only
			distanceStr = "0";
		}
		double distance = Double.parseDouble(distanceStr);
		ITableDataFrame frame = getFrame();
		RDataTable storeFrame = getStore();
		boolean storeResults = (storeFrame != null);
		// this is an R frame for everything exact matches
		// but we ignore exact matches for all other operations anyway...
		boolean canOptimize = (frame instanceof RDataTable);
		
		// we may have the alias
		databaseId = testDatabaseId(databaseId, true);
		IDatabaseEngine database = Utility.getDatabase(databaseId);

		try(WriteOWLEngine owlEngine = database.getOWLEngineFactory().getWriteOWL()) {

			// get tables
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("sourceTable"));
			qs.addSelector(new QueryColumnSelector("targetTable"));
			qs.addSelector(new QueryColumnSelector("sourceCol"));
			qs.addSelector(new QueryColumnSelector("targetCol"));
			qs.addSelector(new QueryColumnSelector("distance"));
	
			// add a filter
			NounMetadata lComparison = new NounMetadata(new QueryColumnSelector("distance"), PixelDataType.COLUMN);
			NounMetadata rComparison = new NounMetadata(distance, PixelDataType.CONST_DECIMAL);
			SimpleQueryFilter filter = new SimpleQueryFilter(lComparison, ">=" , rComparison);
			qs.addExplicitFilter(filter);
			
			int counter = 0;
			logger.info("Retrieving values to insert");
			IRawSelectWrapper iterator = null;
			try {
				iterator = frame.query(qs);
				while(iterator.hasNext()) {
					if(counter % 100 == 0) {
						logger.info("Adding relationship : #" + (counter+1));
					}
					IHeadersDataRow row = iterator.next();
					Object[] values = row.getValues();
					
					String startT = values[0].toString();
					String endT = values[1].toString();
					String startC = values[2].toString();
					String endC = values[3].toString();
					double relDistance = ((Number) values[4]).doubleValue();
					
					// generate the relationship
					String rel = startT + "." + startC + "." + endT + "." + endC;
					
					// add the relationship
					owlEngine.addRelation(startT, endT, rel);
					counter++;
				}
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(iterator != null) {
					try {
						iterator.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
	
			logger.info("Done adding relationships");
			logger.info("Total relationships added = " + counter);
	
			try {
				// commit all the changes
				logger.info("Committing relationships");
				owlEngine.commit();
				owlEngine.export();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
				noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to add the relationships", 
						PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				return noun;
			}
		
		} catch (IOException | InterruptedException e1) {
			classLogger.error(Constants.STACKTRACE, e1);
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to modify the OWL", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		if(storeResults) {
			logger.info("Storing processed results");
			if(canOptimize) {
				optimzieStore((RDataTable) frame, distance, storeFrame, logger);
			} else {
				SelectQueryStruct storeQs = new SelectQueryStruct();
				storeQs.addSelector(new QueryColumnSelector("sourceTable"));
				storeQs.addSelector(new QueryColumnSelector("targetTable"));
				storeQs.addSelector(new QueryColumnSelector("sourceCol"));
				storeQs.addSelector(new QueryColumnSelector("targetCol"));
				storeQs.addSelector(new QueryColumnSelector("distance"));
				
				storeResults(frame, distance, storeQs, storeFrame, logger);
			}
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully adding relationships", 
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////

	/**
	 * Optimized version of storing the results when all the frames are R
	 * @param frame
	 * @param distance
	 * @param storeFrame
	 * @param logger
	 */
	private void optimzieStore(RDataTable frame, double distance, RDataTable storeFrame, Logger logger) {
		IRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);

		String storeFrameName = storeFrame.getName();
		String dataFrameName = frame.getName();
		
		String positiveVarName = "add_" + Utility.getRandomString(6);
		String negativeVarName = "rem_" + Utility.getRandomString(6);

		// store the adds
		String defineVars = positiveVarName + "<-" + dataFrameName + "[" + dataFrameName + "$distance >= " + distance + "];";
		// drop the distance
		// add "add" value
		defineVars += positiveVarName + "[,distance:=NULL];";
		defineVars += positiveVarName + "$action = \"auto_added\";"; 
		
		// same for removes, but different comparator
		defineVars += negativeVarName + "<-" + dataFrameName + "[" + dataFrameName + "$distance < " + distance + "];";
		// drop the distance
		// add "removed" value
		defineVars += negativeVarName + "[,distance:=NULL];";
		defineVars += negativeVarName + "$action = \"auto_removed\";"; 
		
		// note defineVars already ends with ";"
		if(storeFrame.isEmpty()) {
			// merged the 2 frames and assign it to the storeFrameName
			rJavaTranslator.runR(defineVars + storeFrameName + "<-funion(" + positiveVarName + "," + negativeVarName + ");rm(" + positiveVarName + "," + negativeVarName + ");");
			rJavaTranslator.runR("names(" + storeFrameName + ")<-" + 
					RSyntaxHelper.createStringRColVec(new String[]{"sourceTable","sourceCol","targetTable","targetCol","action"}));
			ImportUtility.parseTableColumnsAndTypesToFlatTable(storeFrame.getMetaData(), 
					new String[]{"sourceTable","sourceCol","targetTable","targetCol","action"},
					new String[]{"STRING", "STRING", "STRING", "STRING", "STRING"}, storeFrameName);
		} else {
			// merge each frame into the store frame
			// must have cols in same order
			String storeFrameNameString = storeFrameName + "[, c('sourceTable','sourceCol','targetTable','targetCol','action')]";
			String positiveVarNameString = positiveVarName + "[, c('sourceTable','sourceCol','targetTable','targetCol','action')]";
			String negVarNameString = negativeVarName + "[, c('sourceTable','sourceCol','targetTable','targetCol','action')]";
			rJavaTranslator.runR(defineVars + storeFrameName + "<-funion(" + storeFrameNameString + "," + positiveVarNameString + ");"
					+ storeFrameName + "<-funion(" + storeFrameNameString + "," + negVarNameString + ");rm(" + positiveVarName + "," + negativeVarName + ");");
		}
	}
	
	/**
	 * Store the results by looping through the data
	 */
	private void storeResults(ITableDataFrame frame, double distance, SelectQueryStruct qs, RDataTable storeFrame, Logger logger) {
		List<String> startTList = new Vector<String>();
		List<String> endTList = new Vector<String>();
		List<String> startCList = new Vector<String>();
		List<String> endCList = new Vector<String>();
		List<String> actionList = new Vector<String>();

		int counter = 0;
		IRawSelectWrapper iterator = null;
		try {
			iterator = frame.query(qs);
			while(iterator.hasNext()) {
				IHeadersDataRow row = iterator.next();
				Object[] values = row.getValues();
				
				String startT = values[0].toString();
				String endT = values[1].toString();
				String startC = values[2].toString();
				String endC = values[3].toString();
				double relDistance = ((Number) values[4]).doubleValue();
				String action = "auto_removed";
				
				if(relDistance >= distance) {
					action = "auto_added";
				}
				
				startTList.add(startT);
				endTList.add(endT);
				startCList.add(startC);
				endCList.add(endC);
				actionList.add(action);

				counter++;
				
				if(counter % 50 == 0) {
					storeUserInputs(logger, startTList, startCList, endTList, endCList, actionList);
					
					// now clear
					counter = 0;
					startTList.clear();
					endTList.clear();
					startCList.clear();
					endCList.clear();
					actionList.clear();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(e.getMessage());
		} finally {
			if(iterator != null) {
				try {
					iterator.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// we are done looping
		// there may be additional values
		// that need to be stored
		if(counter > 0) {
			storeUserInputs(logger, startTList, startCList, endTList, endCList, actionList);
		}
	}
	
	/**
	 * Get the frame that contains the information we want to add
	 * @return
	 */
	private ITableDataFrame getFrame() {
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[1]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<Object> frames = this.curRow.getValuesOfType(PixelDataType.FRAME);
		if(frames != null && !frames.isEmpty()) {
			return (ITableDataFrame) frames.get(0);
		}
		
		throw new IllegalArgumentException("Need to define the frame which contains the inforamtion to bulk insert");
	}

}
