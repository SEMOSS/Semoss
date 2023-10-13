package prerna.reactor.frame.r;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class SemanticBlendingReactor extends AbstractRFrameReactor {

	/**
	 * This reactor runs the semantic blending routine on a given column and flushes the results out as a table
	 * The inputs to the reactor are: 
	 * 1) the columns
	 * 2) the number of results to be displayed (defaults to 3 if none is entered)
	 * 3) the number of random values to use in the routine (defaults to 20 if none is entered)
	 * 4) boolean indicator if we want to create an r data table, otherwise just return table of results; true indicates widget is used; defaults to false
	 * 5) name for r data table, if one is to be created
	 */
	
	private static final String CLASS_NAME = SemanticBlendingReactor.class.getName();
	
	// keys used to retrieve user input
	// determine whether using semantic blending or widget
	// default to false
	private static final String GENERATE_FRAME = "genFrame";
	private static final String FRAME_NAME = "frameName";

	public SemanticBlendingReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.NUM_DISPLAY.getKey(), ReactorKeysEnum.RANDOM_VALS.getKey(), GENERATE_FRAME, FRAME_NAME };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		// initialize the rJavaTranslator
		init();
		
		// need to make sure that the WikidataR package is installed before running this method
		String[] packages = new String[] {  "WikidataR","WikipediR", "httr", "curl", "jsonlite" };
		this.rJavaTranslator.checkPackages(packages);
				
		// get frame
		ITableDataFrame frame = getFrame();
		
		// we have an input to indicate whether semantic blending
		// or widget is being used
		// we generate an r data frame for the widget
		// for widget, rDataTableIndicator is true
		boolean generateFrameIndicator = getGenerateFrameIndicator();

		// get other inputs
		// the first input is the columns
		String[] rawColumns = getColumns();

		// check to make sure they are strings
		List<String> stringColumns = new Vector<String>();
		OwlTemporalEngineMeta meta = frame.getMetaData();
		for (int i = 0; i < rawColumns.length; i++) {
			String column = rawColumns[i];
			SemossDataType dataType = meta.getHeaderTypeAsEnum(meta.getUniqueNameFromAlias(column));
			if (dataType == SemossDataType.STRING || dataType == SemossDataType.FACTOR) {
				stringColumns.add(column);
			}
		}
		if (stringColumns.size() == 0) {
			throw new IllegalArgumentException("Predict Column headers only supports String values!");
		}
		String[] columns = stringColumns.toArray(new String[0]);

		// get the number of results to display
		String numDisplayString = getNumResults();

		// get the number of random values to use in the routine
		String randomValsString = getNumRandomVals();
		
		// build a query struct so that we can query and limit the number of values being passed into the method
		// this will also keep track of the columns
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.setLimit(((Number) Double.parseDouble(randomValsString)).longValue());
		for (int i = 0; i < columns.length; i++) {
			qs.addSelector(new QueryColumnSelector(columns[i]));
		}
		
		// create an r data frame (in r) using this querystruct and get the name of the variable
		String dfName = rJavaTranslator.generateRDataTableVariable(frame, qs);
		logger.info("Done generating random subset");
		
		// this will define the column numbers that we are selecting from our frame to run through the routine
		// the r routine uses column numbers rather than names
		StringBuilder colSelectSb = new StringBuilder("c(");

		// format: c(1,2)
		for (int i = 0; i < columns.length; i++) {
			colSelectSb.append((i + 1) + ",");
		}
		
		// remove the last comma and add an end parentheses
		int remove = colSelectSb.length() - 1;
		String colSelectString = colSelectSb.substring(0, remove) + ")";

		// construct a new dataframe to hold the results of the r script
		String df2 = "PredictionTable" + Utility.getRandomString(10);

		StringBuilder rsb = new StringBuilder();

		// determine the path and source the script
		String baseRScriptPath = getBaseFolder() + "\\" + Constants.R_BASE_FOLDER + "\\" + "AnalyticsRoutineScripts";
		String rScriptPath = (baseRScriptPath + "\\" + "master_concept.R").replace("\\", "/");
		String sourceScript = "source(\"" + rScriptPath + "\");";
		rsb.append(sourceScript);
		// run the function
		// function script: PredictionTable<- concept_mgr(frame,c(1,2),1,20);
		rsb.append( df2 + " <- concept_mgr(" + dfName + "," + colSelectString + "," + numDisplayString + "," + randomValsString + ");");
		// results should be in a data frame
		rsb.append(RSyntaxHelper.asDataTable(df2, df2));
		// clean up r temp variables
		rsb.append("rm(" + dfName + ", concept_mgr, concept_xray, endLibs, "
				+ "get_claims,get_concept, get_wiki_ids, is.letter, " + "most_frequent_concept, span, startLibs);");
		rsb.append("gc();");
		logger.info("Running semantic blending script");
		logger.info("This process may take a few minutes depending on the type of data and internet speed");
		this.rJavaTranslator.runR(rsb.toString());
		this.addExecutedCode(rsb.toString());

		// send to GA to store semantic names for predictions
//		String[] colNamesGA = { "Original_Column", "Predicted_Concept", "Prob", "URL" };
//		Map<String, Object> tableGA = this.rJavaTranslator.flushFrameAsTable(df2, colNamesGA);
//		UserTrackerFactory.getInstance().addNewLogicalNames(tableGA, columns, frame);
		
		// if we are running semantic blending
		if (!generateFrameIndicator) {
			// these are the column names for the results
			String[] colNames = { "Predicted_Concept", "Prob", "URL" };
			Map<String, Object> table = this.rJavaTranslator.flushFrameAsTable(df2, colNames);
			return new NounMetadata(table, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.WIKI_LOGICAL_NAMES);
		} else {
			// we are not running semantic blending; we are running the widget
			// need to make a new r table to store this info so we can later query it
			RDataTable resultsTable = new RDataTable(this.insight.getRJavaTranslator(logger), df2);
			// create the new frame meta
			OwlTemporalEngineMeta metaData = resultsTable.getMetaData();
			metaData.addVertex(df2);
			metaData.setPrimKeyToVertex(df2, true);
			String uniqueHeader = df2 + "__" + "Original_Column";
			metaData.addProperty(df2, uniqueHeader);
			metaData.setAliasToProperty(uniqueHeader, "Original_Column");
			metaData.setDataTypeToProperty(uniqueHeader, SemossDataType.STRING.toString());
			uniqueHeader = df2 + "__" + "Predicted_Concept";
			metaData.addProperty(df2, uniqueHeader);
			metaData.setAliasToProperty(uniqueHeader, "Predicted_Concept");
			metaData.setDataTypeToProperty(uniqueHeader, SemossDataType.STRING.toString());
			uniqueHeader = df2 + "__" + "URL";
			metaData.addProperty(df2, uniqueHeader);
			metaData.setAliasToProperty(uniqueHeader, "URL");
			metaData.setDataTypeToProperty(uniqueHeader, SemossDataType.STRING.toString());
			uniqueHeader = df2 + "__" + "Prob";
			metaData.addProperty(df2, uniqueHeader);
			metaData.setAliasToProperty(uniqueHeader, "Prob");
			metaData.setDataTypeToProperty(uniqueHeader, SemossDataType.DOUBLE.toString());

			// store the r variable
			NounMetadata frameNoun = new NounMetadata(resultsTable, PixelDataType.FRAME);
			this.storeVariable(getRDataTableName(), frameNoun);
			return frameNoun;
		}
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String[] getColumns() {
		GenRowStruct columnGrs = this.store.getNoun(ReactorKeysEnum.COLUMNS.getKey());
		if (columnGrs.size() > 0) {
			String[] columns = new String[columnGrs.size()];
			for (int i = 0; i < columnGrs.size(); i++) {
				String column = columnGrs.get(i).toString();
				columns[i] = column;
			}
			return columns;
		}
		throw new IllegalArgumentException("Need to define column to run semantic blending on");
	}
	
	private String getNumResults() {
		GenRowStruct numDisplayGrs = this.store.getNoun(ReactorKeysEnum.NUM_DISPLAY.getKey());
		if (numDisplayGrs != null) {
			if (numDisplayGrs.size() > 0) {
				return numDisplayGrs.get(0).toString();
			}
		}
		//default to 3
		return "3";
	}
	
	private String getNumRandomVals() {
		GenRowStruct randomValsGrs = this.store.getNoun(ReactorKeysEnum.RANDOM_VALS.getKey());
		if (randomValsGrs != null) {
			if (randomValsGrs.size() > 0) {
				return randomValsGrs.get(0).toString();
			}
		}
		// default to 20
		return "20";
	}
	
	private boolean getGenerateFrameIndicator() {
		// see if we are using semantic blending or widget
		// true indicates to use widget
		// default to false (semantic blending)
		GenRowStruct rGrs = this.store.getNoun(GENERATE_FRAME);
		if (rGrs != null) {
			if (rGrs.size() > 0) {
				return (Boolean)rGrs.get(0);
			}
		}
		return false;
	}
	
	private String getRDataTableName() {
		// only get the RDataFrame name if we have determined that we would like to create an RDataFrame
		GenRowStruct nameGrs = this.store.getNoun(FRAME_NAME);
		if (nameGrs != null) {
			if (nameGrs.size() > 0) {
				return nameGrs.get(0).toString();
			}
		}
		// default to "predictionFrame"
		return "predictionFrame";
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(GENERATE_FRAME)) {
			return "Boolean indicator of whether an RDataFrame should be created - defaults to false";
		} if (key.equals(FRAME_NAME)) {
			return "The name for the RDataFrame, if one is to be created";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}