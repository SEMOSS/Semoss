package prerna.reactor.algorithms;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.om.InsightPanel;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RunImpliedInsightsReactor extends AbstractRFrameReactor {

	public RunImpliedInsightsReactor() {
		this.keysToGet = new String[] {  };
	}

	@Override
	public NounMetadata execute() {
		// set up the class
		init();
		organizeKeys();
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		String dtName = frame.getName();
		boolean implicitFilter = false;
		String dtNameIF = "dtFiltered" + Utility.getRandomString(6);
		
		// check if the required packages are installed
		String[] packages = new String[] { "data.table" , "arules" , "FSelector" , "rlang" , "tidyselect" ,  "skimr" , "HDoutliers" , "lubridate" };
		this.rJavaTranslator.checkPackages(packages);

		// check if there are filters on the frame. if so then need to run algorithm on subsetted data
		GenRowFilters filters = frame.getFrameFilters();
		if(!filters.isEmpty()) {
			// create a new qs to retrieve filtered frame
			SelectQueryStruct qs = new SelectQueryStruct();
			List<String> selectedCols = Arrays.asList(frame.getColumnHeaders());
			for(String s : selectedCols) {
				qs.addSelector(new QueryColumnSelector(s));
			}
			qs.setImplicitFilters(filters);
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, meta);
			RInterpreter interp = new RInterpreter();
			interp.setQueryStruct(qs);
			interp.setDataTableName(dtName);
			interp.setColDataTypes(meta.getHeaderToTypeMap());
			String query = interp.composeQuery();
			this.rJavaTranslator.runR(dtNameIF + "<- {" + query + "}");
			implicitFilter = true;
			
			//cleanup the temp r variable in the query var
			this.rJavaTranslator.runR("rm(" + query.split(" <-")[0] + ");gc();");
		}
		String targetDt = implicitFilter ? dtNameIF : dtName;

		// source the r script
		List<String> scriptFilePaths = new Vector<String>();
		scriptFilePaths.add(getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\implied_insights.R");
		scriptFilePaths.add(getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\ImputeData.R");
		for(String path : scriptFilePaths) {
			path = path.replace("\\", "/");
			this.rJavaTranslator.runR("source(\"" + path + "\");");
		}
		
		// run the summary frame functions
		String summaryFrame = getSummaryFrame(targetDt);
		Map<String,String> allSummaryFrames = formatSummaryFrame(summaryFrame);
		
		// run the outlier frame functions
		String makeupFrame = getMakeupFrame(targetDt);
		
		// get the frequent itemsets
		String frequentItemsetsFrame = getFrequentItemsetsFrame(targetDt);		
		
		// format and return the noun
		String[] retData = new String[5];
		retData[0] = allSummaryFrames.get("NUMBER");
		retData[1] = allSummaryFrames.get("STRING");
		retData[2] = allSummaryFrames.get("DATE");
		retData[3] = makeupFrame;
		retData[4] = frequentItemsetsFrame;
		
		// format and return the noun
		List<NounMetadata> tasks = new Vector<>();
		
		// make sure all returned frames are proper semoss frames
		for(String newFrame : retData) {
			NounMetadata noun = null;
			if(newFrame != null && !newFrame.isEmpty()) {
				this.rJavaTranslator.runR(RSyntaxHelper.asDataTable(newFrame, newFrame));
				
				// make sure the table is not empty
				int rowCount = this.rJavaTranslator.getInt("nrow(" + newFrame + ")");
				if(rowCount > 0) {
					RDataTable newTable = createNewFrameFromVariable(newFrame);
					noun = new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME);
					this.insight.getVarStore().put(newFrame, noun);
				}
				this.rJavaTranslator.runR("saveRDS(" + targetDt + ",\"C:/workspace/movie_df.rds\");");
			}
			tasks.add(noun);
		}
		
		// return as array
		return new NounMetadata(tasks, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
	
	private String getMakeupFrame(String targetDt) {
		// name of the results table is what we pass to the FE
		String results = "MakeupFrame_" + Utility.getRandomString(10);
		StringBuilder sb = new StringBuilder(results);
		
		sb.append(" <- get_dataset_makeup(")
		  .append(targetDt)
		  .append(");");
		
		this.rJavaTranslator.runR(sb.toString());
		
		return results;
	}

	private String getFrequentItemsetsFrame(String targetDt) {
		// the name of the results table is what we will be passing to the FE
		String results = "frequentItemsetsFrame_" + Utility.getRandomString(10);
		String targetDf = "targetDf" + Utility.getRandomString(10);

		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();

		// run function
		rsb.append(RSyntaxHelper.asDataFrame(targetDf, targetDt));
		rsb.append(results + " <- get_frequent_itemsets(" + targetDf + ");");

		// run the script
		this.rJavaTranslator.runR(rsb.toString());
		this.rJavaTranslator.executeEmptyR("rm(" + targetDf + "); gc();");
		
		// return new frame
		return results;
	}

	private Map<String,String> formatSummaryFrame(String summaryFrame) {
		// get the possible types
		String possibleTypesVar = "possibleTypesVar" + Utility.getRandomString(6);
		this.rJavaTranslator.runR(possibleTypesVar + " <- names(" + summaryFrame + ");");
		String[] possibleTypes = this.rJavaTranslator.getStringArray(possibleTypesVar);
		this.rJavaTranslator.executeEmptyR("rm(" + possibleTypesVar + "); gc();");
		
		// get the cleaned types for UI
		String[] typesCleaned =  getCleanTypes(possibleTypes);
		Map<String,String> retList = new HashMap<String,String>();

		// loop through possible types and add it to the return
		for(int i = 0; i < possibleTypes.length; i ++) {
			// get type info
			String tableName = summaryFrame + "$" + possibleTypes[i];
			
			// put it in a name
			String newTableName = summaryFrame + "_" + typesCleaned[i];
			this.rJavaTranslator.runR(newTableName + " <- " + tableName + ";");
			
			// add it to the list
			retList.put(typesCleaned[i],newTableName);
		}
		
		return retList;
		
	}

	private String[] getCleanTypes(String[] possibleTypes) {
		int length = possibleTypes.length;
		String[] cleanedTypes = new String[length];
		
		for(int i = 0; i < length; i ++) {
			if(possibleTypes[i].equals("number")) {
				cleanedTypes[i] = "NUMBER";
			} else if(possibleTypes[i].equals("character")) {
				cleanedTypes[i] = "STRING";
			} else if(possibleTypes[i].equals("Date")) {
				cleanedTypes[i] = "DATE";
			} else {
				cleanedTypes[i] = "OTHER";
			}
		}
		
		return cleanedTypes;
	}

	private String getSummaryFrame(String targetDt) {
		// the name of the results table is what we will be passing to the FE
		String results = "sumFrame" + Utility.getRandomString(10);

		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();

		// run function
		rsb.append(results + " <- get_df_scan(" + targetDt + ");");

		// run the script
		this.rJavaTranslator.runR(rsb.toString());

		// return new frame
		return results;
	}
	
	
	protected InsightPanel getInsightPanel() {
		InsightPanel panel = null;

		// see if panel was passed via generic reactor
		GenRowStruct genericGrs = this.store.getNoun(ReactorKeysEnum.PANEL.getKey());
		if (genericGrs != null && !genericGrs.isEmpty()) {
			String panelId = genericGrs.get(0).toString();
			panel = this.insight.getInsightPanel(panelId);
		}

		if (panel == null) {
			// if not, see if it was passed in the grs
			List<Object> panelNouns = this.curRow.getValuesOfType(PixelDataType.PANEL);
			if (panelNouns != null && !panelNouns.isEmpty()) {
				panel = (InsightPanel) panelNouns.get(0);
			}
		}

		if (panel == null) {
			throw new IllegalArgumentException("Invalid panel id passed into With reactor");
		}

		return panel;
	}
	
}
