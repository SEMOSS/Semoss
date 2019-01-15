package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RAprioriReactor extends AbstractRFrameReactor {
	/**
	 * RunAssociatedLearning(attributes = ["Class_1", "Sex", "Survived","Age"], conf = [0.8],support = [0.005], rhsAttribute=["Survived"], panel=[999]);
	 * RunAssociatedLearning(attributes = ["itemDescription"], idAttributes = ["Member_number","Date_1"], panel=[99]);
	 * RunAssociatedLearning(attributes = ["itemDescription"], idAttributes = ["Member_number","Date_1"], conf = [0.1],support = [0.001], panel=[99]);
	 * Input keys: 
	 * 		1. attributes (required) 
	 * 		2. idAttributes (optional)
	 * 		3. conf (optional) - must be within (0,1] range (default: 0.8)
	 * 		4. support (optional) - must be within (0,1] range (default: 0.1)
	 * 		5. maxlen (optional) - an integer value for the maximal number of items per item set (default: 10 items)
	 * 		6. sortby (optional) - must be either "confidence" or "lift" (default: lift) 
	 * 		7. lhsAttributes (optional) - list of attributes (1 or many) being requested to appear on the left hand side of the rules 
	 * 		8. rhsAttribute (optional) - 1 attribute being requested to appear on the right hand side of the rules 
	 * 
	 */
	
	private static final String CLASS_NAME = RAprioriReactor.class.getName();

	private static final String IDATTRIBUTES = "idAttributes";
	private static final String CONFIDENCE = "conf";
	private static final String SUPPORT = "support";
	private static final String MAXLEN = "maxlen";
	private static final String SORTBY = "sortby";
	private static final String LHSATTRIBUTES = "lhsAttributes";
	private static final String RHSATTRIBUTE = "rhsAttribute";

	public RAprioriReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ATTRIBUTES.getKey(), IDATTRIBUTES, CONFIDENCE, 
				SUPPORT, MAXLEN, SORTBY, LHSATTRIBUTES, RHSATTRIBUTE, ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] { "data.table", "dplyr", "arules" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		String dtName = frame.getName();
		List<String> colNames = Arrays.asList(frame.getColumnNames());
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		boolean implicitFilter = false;
		String dtNameIF = "dtFiltered" + Utility.getRandomString(6);
		StringBuilder sb = new StringBuilder();

		// get inputs from pixel command
		String panelId = getPanelId();
		double conf = getInputDouble(CONFIDENCE);
		double supp = getInputDouble(SUPPORT);
		double maxlen = getInputDouble(MAXLEN);
		List<String> attributesList = getInputList("0");
		List<String> idAttributesList = getInputList(IDATTRIBUTES);
		List<String> lhsVarList = getInputList(LHSATTRIBUTES);
		if (lhsVarList != null & lhsVarList.size() > 0) {
			for (int i = 0; i < lhsVarList.size(); i++) {
				if (!colNames.contains(lhsVarList.get(i)))
					throw new IllegalArgumentException("LHS attribute(s) contain invalid column name(s).");
			}
		}
		String sortBy = getInputString(SORTBY);
		String rhsVar = getInputString(RHSATTRIBUTE);
		if (rhsVar != null && rhsVar != "") {
			if (!colNames.contains(rhsVar)) {
				throw new IllegalArgumentException("RHS attribut is an invalid column name.");
			}
		}

		String attrList_R = "attrList" + Utility.getRandomString(8);
		String attrListStr = "'" + attributesList.toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", "','")	+ "'";
		sb.append(attrList_R + " <- c(" + attrListStr + ");");
		StringBuilder substr = new StringBuilder();
		if (idAttributesList != null && idAttributesList.size() > 0) {
			String idAttributesListStr = "'" + idAttributesList.toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", "','") + "'";
			substr.append(",transactionIdList = c(" + idAttributesListStr + ")");
		}
		if (conf > 0) {
			substr.append(",confidence = " + conf);
		}
		if (supp > 0) {
			substr.append(",support = " + supp);
		}
		if (maxlen > 0) {
			substr.append(",maxlen = " + maxlen);
		}
		if (sortBy != null && sortBy != "") {
			substr.append(",sortBy = '" + sortBy.toLowerCase() + "'");
		}
		if (rhsVar != null && rhsVar != "") {
			substr.append(",rhsSpecified = '" + rhsVar + "'");
		}
		if (lhsVarList != null && lhsVarList.size() > 0) {
			String lhsVarListStr = "'" + lhsVarList.toString().replace("[", "").replace("]", "").replace(" ", "").replace(",", "','") + "'";
			substr.append(",lhsSpecified = c(" + lhsVarListStr + ")");
		}

		// check if there are filters on the frame. if so then need to run
		// algorithm on subsetted data
		if (!frame.getFrameFilters().isEmpty()) {
			// create a new qs to retrieve filtered frame
			SelectQueryStruct qs = new SelectQueryStruct();
			List<String> selectedCols = new ArrayList<String>(attributesList);
			selectedCols.addAll(idAttributesList);
			for (String s : selectedCols) {
				qs.addSelector(new QueryColumnSelector(s));
			}
			qs.setImplicitFilters(frame.getFrameFilters());
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, meta);
			RInterpreter interp = new RInterpreter();
			interp.setQueryStruct(qs);
			interp.setDataTableName(dtName);
			interp.setColDataTypes(meta.getHeaderToTypeMap());
			String query = interp.composeQuery();
			this.rJavaTranslator.runR(dtNameIF + "<- {" + query + "}");
			implicitFilter = true;

			// cleanup the temp r variable in the query var
			this.rJavaTranslator.runR("rm(" + query.split(" <-")[0] + ");gc();");
		}

		String targetDt = implicitFilter ? dtNameIF : dtName;

		// apriori r script
		String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\Apriori.R";
		scriptFilePath = scriptFilePath.replace("\\", "/");
		sb.append("source(\"" + scriptFilePath + "\");");

		// set call to R function
		String temp_R = "temp_R" + Utility.getRandomString(8);
		if (substr.indexOf(",") == 0) {
			substr.deleteCharAt(0);
		}
		sb.append(temp_R + " <- runApriori( " + targetDt + "," + attrList_R + "," + substr + ");");
		String rulesLength_R = "rulesLength" + Utility.getRandomString(8);
		sb.append(rulesLength_R + "<-" + temp_R + "$rulesLength;");
		String rulesDt_R = "rulesDt" + Utility.getRandomString(8);
		sb.append(rulesDt_R + "<-" + temp_R + "$rulesDt;");

		// execute R
		this.rJavaTranslator.runR(sb.toString());
		int ruleslength = this.rJavaTranslator.getInt(rulesLength_R);

		// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + attrList_R + "," + temp_R + "," + rulesLength_R + "," + dtNameIF + ",runApriori);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());

		if (ruleslength == 0) {
			throw new IllegalArgumentException("Assocation Learning Algorithm ran successfully, but no results were found.");
		}

		String[] rulesDtColNames = this.rJavaTranslator.getColumns(rulesDt_R);
		List<Object[]> data = this.rJavaTranslator.getBulkDataRow(rulesDt_R, rulesDtColNames);
		this.rJavaTranslator.runR("rm(" + rulesDt_R + ");gc();");

		// task data includes task options
		ITask taskData = ConstantTaskCreationHelper.getGridData(panelId, rulesDtColNames, data);
		this.insight.getTaskStore().addTask(taskData);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"AssociatedLearning", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		NounMetadata noun = new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		noun.addAdditionalReturn(
				new NounMetadata("Associated Learning ran successfully!", 
						PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
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
			if (inputName == SORTBY && !value.equalsIgnoreCase("confidence") && !value.equalsIgnoreCase("lift")) {
				throw new IllegalArgumentException("Sortby variable must be either 'confidence' or 'lift'.");
			}
		}
		return value;
	}

	private List<String> getInputList(String input) {
		List<String> retList = new ArrayList<String>();

		// check if list input was entered with key or not
		GenRowStruct columnGrs = (input == "0") ? this.store.getNoun(keysToGet[0]) : this.store.getNoun(input);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				retList.add(noun.getValue().toString());
			}
		} else {
			if (input == "0") {
				throw new IllegalArgumentException("Attribute(s) that make up a transaction must be specified.");
			}
		}

		return retList;
	}

	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(this.keysToGet[8]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}

}
