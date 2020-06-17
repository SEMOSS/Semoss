package prerna.sablecc2.reactor.algorithms;

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
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;

public class RunImpliedInsightsReactor extends AbstractRFrameReactor {

	public RunImpliedInsightsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PANEL.getKey() };
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
		InsightPanel panel = getInsightPanel();
		
		// check if the required packages are installed
		String[] packages = new String[] { "data.table" , "arules" , "Boruta" , "rlang" , "tidyselect" ,  "skimr" };
		this.rJavaTranslator.checkPackages(packages);

		// check if there are filters on the frame. if so then need to run algorithm on subsetted data
		GenRowFilters filters = panel.getPanelFilters();
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

		// run the summary frame functions
		String summaryFrame = getSummaryFrame(targetDt);
		List<Map<String,Object>> summaryRetMap = formatSummaryFrame(summaryFrame);
		
		// run the outlier frame functions
		String outlierFrame = getOutlierFrame(targetDt);
		String[] outlierColNames = this.rJavaTranslator.getColumns(outlierFrame);
		List<Object[]> outlierData = this.rJavaTranslator.getBulkDataRow(outlierFrame, outlierColNames);
		Map<String, Object> outlierRetMap = new HashMap<>();
		outlierRetMap.put("headers", outlierColNames);
		outlierRetMap.put("values", outlierData);
		
		// garbage cleanup
		this.rJavaTranslator.executeEmptyR("rm(" + outlierFrame + " , " + summaryFrame + "); gc();");
		
		// format and return the noun
		List<NounMetadata> tasks = new Vector<>();
		NounMetadata noun1 = new NounMetadata(summaryRetMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
		NounMetadata noun2 = new NounMetadata(outlierRetMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
		
		tasks.add(noun1);
		tasks.add(noun2);
		return new NounMetadata(tasks, PixelDataType.VECTOR, PixelOperationType.VECTOR);
	}

	private List<Map<String, Object>> formatSummaryFrame(String summaryFrame) {
		// get the possible types
		String possibleTypesVar = "possibleTypesVar" + Utility.getRandomString(6);
		this.rJavaTranslator.runR(possibleTypesVar + " <- names(" + summaryFrame + ");");
		String[] possibleTypes = this.rJavaTranslator.getStringArray(possibleTypesVar);
		this.rJavaTranslator.executeEmptyR("rm(" + possibleTypesVar + "); gc();");
		
		// get the cleaned types for UI
		String[] typesCleaned =  getCleanTypes(possibleTypes);
		List<Map<String,Object>> retList = new Vector<Map<String,Object>>();

		// loop through possible types and add it to the return
		for(int i = 0; i < possibleTypes.length; i ++) {
			// get type info
			String tableName = summaryFrame + "$" + possibleTypes[i];
			String type = typesCleaned[i];
			
			// get the data and headers
			String[] typeSummaryColNames = this.rJavaTranslator.getColumns(tableName);
			List<Object[]> typeSummaryData = this.rJavaTranslator.getBulkDataRow(tableName, typeSummaryColNames);
			
			// add it to a map
			Map<String, Object> typeRetMap = new HashMap<>();
			typeRetMap.put("datatype", type);
			typeRetMap.put("headers", typeSummaryColNames);
			typeRetMap.put("values", typeSummaryData);
			
			// add that to our return list
			retList.add(typeRetMap);
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

		// source the r script that will run the outlier and impute routine
		String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\implied_insights.R";
		scriptFilePath = scriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + scriptFilePath + "\");");

		// run function
		rsb.append(RSyntaxHelper.asDataTable(targetDt, targetDt));
		rsb.append(results + " <- get_df_scan(" + targetDt + ");");

		// run the script
		this.rJavaTranslator.runR(rsb.toString());

		// return new frame
		return results;
	}
	
	private String getOutlierFrame(String targetDt) {
		// the name of the results table is what we will be passing to the FE
		String results = "outlierFrame" + Utility.getRandomString(10);

		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();

		// source the r script that will run the outlier and impute routine
		String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\implied_insights.R";
		scriptFilePath = scriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + scriptFilePath + "\");");

		// run function
		rsb.append(results + " <- get_dataset_outliers(" + targetDt + ");");

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

		// or was passed in via a "|"
		GenRowStruct pipedGrs = this.store.getNoun(PixelDataType.PANEL.toString());
		if (pipedGrs != null && !pipedGrs.isEmpty()) {
			panel = (InsightPanel) pipedGrs.get(0);
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
