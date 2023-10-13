package prerna.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RunAssociatedLearningReactor extends AbstractRFrameReactor {
	
	/**
	 * RunAssociatedLearning(ruleSide=["Outcome"], column=["Nominated"] ,
	 * values=["Y"] , attributes=["Genre" , "Nominated" ,"Rating"], conf =
	 * [0.5],support = [0.005],lift=[1], panel=[0]);
	 */

	private static final String RULE_SIDE = "ruleSide";
	private static final String CONFIDENCE = "conf";
	private static final String SUPPORT = "support";
	private static final String LIFT = "lift";

	public RunAssociatedLearningReactor() {
		this.keysToGet = new String[] { RULE_SIDE, ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.VALUES.getKey(),
				ReactorKeysEnum.ATTRIBUTES.getKey(), CONFIDENCE, SUPPORT, LIFT, ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] { "data.table", "dplyr", "arules", "stringr" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getName();
		List<String> colNames = Arrays.asList(frame.getColumnNames());
		boolean implicitFilter = false;
		String dtNameIF = "dtFiltered" + Utility.getRandomString(6);
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();

		// get inputs from pixel command
		String panelId = getPanelId();
		String column = getInputString(ReactorKeysEnum.COLUMN.getKey());
		List<String> values = getInputList(ReactorKeysEnum.VALUES.getKey());
		List<String> attrs = getInputList(ReactorKeysEnum.ATTRIBUTES.getKey());
		String ruleSide = getInputString(RULE_SIDE).toLowerCase();
		double conf = getInputDouble(CONFIDENCE);
		double supp = getInputDouble(SUPPORT);
		double lift = getInputDouble(LIFT);

		// make sure that column exists
		if (!colNames.contains(column)) {
			throw new IllegalArgumentException("Please select a valid column name");
		}

		// ensure the rule column is in the attrs
		if (!attrs.contains(column)) {
			attrs.add(column);
		}

		// check if there are filters on the frame. if so then need to run
		// algorithm on subsetted data
		if (!frame.getFrameFilters().isEmpty()) {
			// create a new qs to retrieve filtered frame
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector(column));
			qs.setImplicitFilters(frame.getFrameFilters());
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, meta);
			RInterpreter interp = new RInterpreter();
			interp.setQueryStruct(qs);
			interp.setDataTableName(frameName);
			interp.setColDataTypes(meta.getHeaderToTypeMap());
			String query = interp.composeQuery();
			this.rJavaTranslator.runR(dtNameIF + "<- {" + query + "}");
			implicitFilter = true;

			// cleanup the temp r variable in the query var
			this.rJavaTranslator.runR("rm(" + query.split(" <-")[0] + ");gc();");
		}

		String targetDt = implicitFilter ? dtNameIF : frameName;

		String resultsFrame = runAssociatedRulesRScript(targetDt, ruleSide, column, values, attrs, supp, conf, lift);
		int ruleslength = this.rJavaTranslator.getInt("nrow(" + resultsFrame + ")");

		String[] rulesDtColNames = this.rJavaTranslator.getColumns(resultsFrame);
		List<Object[]> data = this.rJavaTranslator.getBulkDataRow(resultsFrame, rulesDtColNames);
		this.rJavaTranslator.runR("rm(" + resultsFrame + ");gc();");

		// task data includes task options
		ITask taskData = ConstantTaskCreationHelper.getGridData(panelId, rulesDtColNames, data);
		this.insight.getTaskStore().addTask(taskData);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, frame, "AssociatedLearning",
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		NounMetadata noun = new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		
		// throw message based on result
		if (ruleslength == 0) {
			noun.addAdditionalReturn(NounMetadata.getErrorNounMessage("Assocation Learning Algorithm ran successfully, but no results were found"));
		} else {
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Associated Learning ran successfully!"));
		}
		
		return noun;
	}

	private String runAssociatedRulesRScript(String frameName, String ruleSide, String column, List<String> values,
			List<String> attrs, double supp, double conf, double lift) {
		// the name of the results table is what we will be passing to the FE
		String resultsFrameName = "ResultsTable" + Utility.getRandomString(10);

		// create a stringbuilder for our r syntax
		StringBuilder rsb = new StringBuilder();

		// source the r script that will run the outlier and impute routine
		String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\association_rules.R";
		scriptFilePath = scriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + scriptFilePath + "\");");

		// create the premise/outcome string
		List<String> valuePairings = new ArrayList<String>();
		for (String v : values) {
			valuePairings.add(column + "=" + v);
		}
		String rule = ruleSide + "=" + RSyntaxHelper.createStringRColVec(valuePairings);

		// move attrs into R vector
		String attrsR = RSyntaxHelper.createStringRColVec(attrs);

		String frameAsDf = frameName + Utility.getRandomString(6);
		rsb.append(RSyntaxHelper.asDataFrame(frameAsDf, frameName));
		rsb.append(resultsFrameName + "<- get_association_rules(" + frameAsDf + ", " + attrsR + ", " + rule
				+ ", support=" + supp + ", confidence=" + conf + ",lift=" + lift + "); ");

		// run the script
		this.rJavaTranslator.runR(rsb.toString());

		// garbage collection
		this.rJavaTranslator.executeEmptyR("rm(" + frameAsDf + "); gc();");

		// return new frame
		return resultsFrameName;

	}

	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	/////////////////////// PIXEL INPUTS //////////////////////////////

	private double getInputDouble(String inputName) {
		GenRowStruct grs = this.store.getNoun(inputName);
		double value = -1.0;
		NounMetadata noun;
		if (grs != null) {
			noun = grs.getNoun(0);
			value = ((Number) noun.getValue()).doubleValue();
		}
		return value;
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

	private List<String> getInputList(String input) {
		List<String> retList = new ArrayList<String>();

		// check if list input was entered with key or not
		GenRowStruct columnGrs = input.equals("0") ? this.store.getNoun(keysToGet[0]) : this.store.getNoun(input);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				retList.add(noun.getValue().toString());
			}
		} else {
			if (input.equals("0")) {
				throw new IllegalArgumentException("Attribute(s) that make up a transaction must be specified.");
			}
		}

		return retList;
	}

	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(ReactorKeysEnum.PANEL.getKey());
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}

}
