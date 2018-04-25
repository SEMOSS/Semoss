package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;

public class RAprioriReactor extends AbstractRFrameReactor {
	
	private static final String CLASS_NAME = RAprioriReactor.class.getName();

	private static final String IDATTRIBUTES = "idAttributes";
	private static final String CONFIDENCE = "conf";
	private static final String SUPPORT = "support";
	private static final String MAXLEN = "maxlen";
	private static final String SORTBY = "sortby";
	private static final String LHSATTRIBUTES = "lhsAttributes";
	private static final String RHSATTRIBUTE = "rhsAttribute";

	private double conf;
	private double supp;
	private double maxlen;
	private List<String> attributesList;
	private List<String> idAttributesList;
	private String sortBy;
	private String rhsVar;
	private List<String> lhsVarList;

	/**
	 * RunAssociatedLearningR(attributes = ["Class_1", "Sex", "Survived","Age"], conf = [0.8],support = [0.005], rhsAttribute=["Survived"], panel=[999]);
	 */

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
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		String dtName = frame.getTableName();
		List<String> colNames = Arrays.asList(frame.getColumnNames());
		String panelId = getPanelId();
		StringBuilder sb = new StringBuilder();

		// get inputs from pixel command
		this.conf = getInputDouble(CONFIDENCE);
		this.supp = getInputDouble(SUPPORT);
		this.maxlen = getInputDouble(MAXLEN);

		this.attributesList = getInputList("0");
		this.idAttributesList = getInputList(IDATTRIBUTES);
		if (idAttributesList != null && idAttributesList.size() > 0 && attributesList.size() != 1) {
			throw new IllegalArgumentException(
					"Expecting only 1 column as attribute, which will serve as transaction items");
		}
		this.lhsVarList = getInputList(LHSATTRIBUTES);
		if (lhsVarList != null & lhsVarList.size() > 0) {
			for (int i = 0; i < lhsVarList.size(); i++) {
				if (!colNames.contains(lhsVarList.get(i)))
					throw new IllegalArgumentException("LHS attribute(s) contain invalid column name(s).");
			}
		}

		this.sortBy = getInputString(SORTBY);
		this.rhsVar = getInputString(RHSATTRIBUTE);
		if (!colNames.contains(this.rhsVar)) {
			throw new IllegalArgumentException("RHS attribut is an invalid column name.");
		}
		
		// dt, attrList, transactionIdList = NULL, support = NULL, confidence =
		// NULL, maxlen = NULL, sortBy = "lift", lhsSpecified = NULL,
		// rhsSpecified = NULL
		String attrList_R = "attrList" + Utility.getRandomString(8);
		String attrListStr  = "'" + this.attributesList.toString().replace("[","").replace("]", "").replace(" ","").replace(",","','") + "'";
		System.out.println(attrListStr);
		sb.append(attrList_R + " <- c(" + attrListStr + ");");
		
		StringBuilder substr = new StringBuilder();
		if (this.idAttributesList != null && this.idAttributesList.size() > 0) {
			String idAttributesListStr  = "'" + this.idAttributesList.toString().replace("[","").replace("]", "").replace(" ","").replace(",","','") + "'";
			substr.append(",transactionIdList = c(" + idAttributesListStr + ")");
		}
		if (this.conf > 0) substr.append("confidence = " + this.conf);
		if (this.supp > 0) substr.append(",support = " + this.supp);
		if (this.maxlen > 0) substr.append(",maxlen = " + this.maxlen);
		if (this.sortBy != null && this.sortBy != "") substr.append(",sortBy = '" + this.sortBy.toLowerCase() + "'");
		if (this.rhsVar != null && this.rhsVar != "") substr.append(",rhsSpecified = '" + this.rhsVar + "'");
		if (this.lhsVarList != null && this.lhsVarList.size() > 0) {
			String lhsVarListStr  = "'" + this.lhsVarList.toString().replace("[","").replace("]", "").replace(" ","").replace(",","','") + "'";
			substr.append(",lhsSpecified = c(" + lhsVarListStr + ")");
		}
		System.out.println(substr.toString());

		// apriori r script
		String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\Apriori.R";
		scriptFilePath = scriptFilePath.replace("\\", "/");
		sb.append("source(\"" + scriptFilePath + "\");");
		
		// set call to R function
		String temp_R = "temp" + Utility.getRandomString(8);
		if (substr == null) {
			sb.append(temp_R + " <- runApriori( " + dtName + "," + attrList_R + ");");
		} else {
			if (substr.indexOf(",") == 0) substr.deleteCharAt(0);
			sb.append(temp_R + " <- runApriori( " + dtName + "," + attrList_R + "," + substr + ");");
		}
		String rulesLength_R = "rulesLength" + Utility.getRandomString(8);
		sb.append(rulesLength_R + "<-" + temp_R + "$rulesLength;");
		String rulesDt_R = "rulesDt" + Utility.getRandomString(8);
		sb.append(rulesDt_R + "<-" + temp_R + "$rulesDt;");
		
		// execute R
		System.out.println(sb.toString());
		this.rJavaTranslator.runR(sb.toString());
		
		int ruleslength = this.rJavaTranslator.getInt(rulesLength_R);
		if (ruleslength == 0) {
			throw new IllegalArgumentException("Assocation Learning Algorithm ran successfully, but no results were found.");
		} else {
			String[] rulesDtColNames = this.rJavaTranslator.getColumns(rulesDt_R);
			List<Object[]> data = this.rJavaTranslator.getBulkDataRow(rulesDt_R, rulesDtColNames);
			//task data includes task options
			Map<String, Object> taskData = getGridData(panelId, rulesDtColNames, data);
			return new NounMetadata(taskData, PixelDataType.TASK, PixelOperationType.TASK_DATA);
		}
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
		if (grs != null) {
			noun = grs.getNoun(0);
			value = noun.getValue().toString();
			if (inputName == SORTBY && (value.toLowerCase() != "confidence" || value.toLowerCase() != "lift")) {
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
