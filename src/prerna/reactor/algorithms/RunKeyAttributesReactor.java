package prerna.reactor.algorithms;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RunKeyAttributesReactor extends AbstractRFrameReactor {

	public RunKeyAttributesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey() };
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
		String col = getInputString(ReactorKeysEnum.COLUMN.getKey());
		
		// check if the required packages are installed
		String[] packages = new String[] { "data.table" , "FSelector" , "lubridate" , "missRanger"};
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
		
		// run the function
		String keyAttributesFrame = getKeyAttributesFrame(targetDt, col);
		
		// return the data
		String[] keyAttributesColNames = this.rJavaTranslator.getColumns(keyAttributesFrame);
		List<Object[]> keyAttributesData = this.rJavaTranslator.getBulkDataRow(keyAttributesFrame, keyAttributesColNames);
		HashMap<String, Object> keyAttributesRetMap = new HashMap<String, Object>();
		keyAttributesRetMap.put("headers", keyAttributesColNames);
		keyAttributesRetMap.put("values", keyAttributesData);
		
		// store it and return name for FE
		return new NounMetadata(keyAttributesRetMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private String getKeyAttributesFrame(String targetDt, String col) {
		// the name of the results table is what we will be passing to the FE
		String results = "keyAttributesFrame_" + Utility.getRandomString(10);
		String targetDf = "targetDf"+Utility.getRandomString(10);
		StringBuilder rsb = new StringBuilder();
		
		// build a frame to return
		rsb.append(RSyntaxHelper.asDataFrame(targetDf, targetDt));
		rsb.append(results + " <- select_features(" + targetDf + ",\"" + col + "\");");
		
		// run the script
		this.rJavaTranslator.runR(rsb.toString());
		
		// gc
		this.rJavaTranslator.executeEmptyR("rm( " + targetDf + " ); gc();");

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
	
	private String getInputString(String inputName) {
		GenRowStruct grs = this.store.getNoun(inputName);
		String value = "";
		NounMetadata noun;
		if (grs != null && grs.size() > 0) {
			noun = grs.getNoun(0);
			value = noun.getValue().toString();
		}
		return value;
	}
	
}
