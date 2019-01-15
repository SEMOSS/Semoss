package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RRandomForestAlgorithmReactor extends AbstractRFrameReactor {
	/**
	 * RunRandomForest(classify=[Species],attributes=["PetalLength","PetalWidth","SepalLength","SepalWidth"], options=["na.action=na.omit","importance=TRUE","ntree=1000"])
	 * RunRandomForest(classify=[race],attributes=["age","workclass","education","marital_status","relationship","sex","capital_gain","capital_loss","income"])
	 */
	private static final String CLASS_NAME = RRandomForestAlgorithmReactor.class.getName();
	private static final String RF_VARIABLE = "RF_VARIABLE_999988888877777";
	private static final String CLASSIFICATION_COLUMN = "classify";
	private static final String OPTIONS = "options";

	public RRandomForestAlgorithmReactor() {
		this.keysToGet = new String[] { CLASSIFICATION_COLUMN, ReactorKeysEnum.ATTRIBUTES.getKey(), OPTIONS};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		init();
		String[] packages = new String[] { "data.table", "randomForest", "dplyr" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		String dtName = frame.getName();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		boolean implicitFilter = false;
		String dtNameIF = "dtFiltered" + Utility.getRandomString(6);
		StringBuilder sb = new StringBuilder();

		// retrieve inputs
		String instanceCol = getClassificationColumn();
		String instanceCol_R = "instanceCol" + Utility.getRandomString(8);
		sb.append(instanceCol_R + "<- \"" + instanceCol + "\";");

		List<String> attributes = getInputList(1);
		if (attributes.contains(instanceCol)) attributes.remove(instanceCol);
		String attributes_R = "attributes" + Utility.getRandomString(8);
		sb.append(attributes_R + "<- " + RSyntaxHelper.createStringRColVec(attributes.toArray()) + ";");
		
		// check if there are filters on the frame. if so then need to run algorithm on subsetted data
		if(!frame.getFrameFilters().isEmpty()) {
			// create a new qs to retrieve filtered frame
			SelectQueryStruct qs = new SelectQueryStruct();
			List<String> selectedCols = new ArrayList<String>(attributes);
			selectedCols.add(instanceCol);
			for(String s : selectedCols) {
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
			
			//cleanup the temp r variable in the query var
			this.rJavaTranslator.runR("rm(" + query.split(" <-")[0] + ");gc();");
		}
		
		String targetDt = implicitFilter ? dtNameIF : dtName;
		
		List<String> options = getInputList(2);
		String options_R = "options" + Utility.getRandomString(8);
		if (options != null && options.size() > 0) {
			sb.append(options_R + "<- \"" + options.toString().replace("[","").replace("]", "") + "\";");
		} else {
			sb.append(options_R + "<- \"\";");
		}

		// random forest r script
		String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\RandomForest.R";
		scriptFilePath = scriptFilePath.replace("\\", "/");
		sb.append("source(\"" + scriptFilePath + "\");");

		// set call to R function
		sb.append(RF_VARIABLE + " <- getRF( " + targetDt + "," + instanceCol_R + "," + attributes_R + ",options=" + options_R + ");");

		// execute R
		this.rJavaTranslator.runR(sb.toString());
		
		//// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + instanceCol_R + "," + attributes_R + "," + options_R + "," + dtNameIF + ",getRF,getRFResults);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());
		
		String rfType = this.rJavaTranslator.getString(RF_VARIABLE + "$type");
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"RandomForest", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		if (new ArrayList<String>(Arrays.asList("classification", "regression")).contains(rfType)) {
			return new NounMetadata(RF_VARIABLE, PixelDataType.VARIABLE);
		} else {
			this.rJavaTranslator.runR("rm(" + RF_VARIABLE + ");gc();");
			throw new IllegalArgumentException("Random forest could not run.");
		}
	}
	
	
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	////////////////////// Input Methods///////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	private String getClassificationColumn() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(CLASSIFICATION_COLUMN);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}

		// else, we assume it is the first column
		if (this.curRow == null || this.curRow.size() == 0) {
			String errorString = "Could not find the column predict";
			throw new IllegalArgumentException(errorString);
		}
		return this.curRow.get(0).toString();
	}

	private List<String> getInputList(int index) {
		List<String> retList = new ArrayList<String>();
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[index]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				retList.add(noun.getValue().toString());
			}
		} else {
			if (index == 1) throw new IllegalArgumentException("Please specify attributes.");
		}
		return retList;
	}

}
