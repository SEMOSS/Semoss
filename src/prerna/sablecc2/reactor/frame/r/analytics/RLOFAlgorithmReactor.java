package prerna.sablecc2.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;

public class RLOFAlgorithmReactor extends AbstractRFrameReactor {
	/*
	 * RunLOF(instance = [Studio], kNeighbors = [10], attributes = ["MovieBudget", "Revenue_Domestic"])
	 * RunLOF(instance = [Studio], kNeighbors = [   "10:12, 5" ], attributes = ["MovieBudget", "Revenue_Domestic"])
	 * RunLOF(instance = [Studio], kNeighbors = ["10,11,12"], attributes = ["MovieBudget", "Revenue_Domestic"])
	 * RunLOF(instance = [Species], uniqInstPerRow = "false", kNeighbors =10, attributes = ["SepalLength", "SepalWidth","PetalLength", "PetalWidth"])
	 * 
	 * Input keys: 
	 * 		1. instance (required) 
	 * 		2. kNeighbors (required) - can be one of 3 types: integer or a list of integers (if list then wrap the whole list in quotes)
	 * 			example: kNeighbors = [10]; kNeighbors = ["5:7"]; kNeighbors = ["5, 6, 7"]
	 * 		3. attributes (required) - must be columns of data type = numeric
	 * 		4. uniqInstPerRow (optional; if not passed in, assumes false) - 
	 * 			if true, then will treat each row in the frame as a unique instance/record; 
	 * 			if false, then will aggregate the data in the attributes columns by the instance column first
	 */
	private static final String CLASS_NAME = RLOFAlgorithmReactor.class.getName();
	
	private static final String K_NEIGHBORS = "kNeighbors"; 
	private static final String UNIQUE_INSTANCE_PER_ROW= "uniqInstPerRow";
	
	private String instanceColumn;
	private List<String> attributeNamesList;
	private double[] kArray;
	
	public RLOFAlgorithmReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.INSTANCE_KEY.getKey(), ReactorKeysEnum.ATTRIBUTES.getKey(), 
				K_NEIGHBORS, UNIQUE_INSTANCE_PER_ROW};
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] { "Rlof", "data.table", "dplyr", "VGAM" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		String dtName = frame.getTableName();
		StringBuilder sb = new StringBuilder();		

		//retrieve inputs
		this.instanceColumn = getInstanceColumn();
		this.attributeNamesList = getAttrList();
		for (String attr : this.attributeNamesList){
			String dataType = meta.getHeaderTypeAsString(dtName + "__" + attr);
			if (!Utility.isNumericType(dataType)) {
				throw new IllegalArgumentException("Attribute columns must be of numeric type.");
			}
		}
		String uniqInstPerRowStr = getUniqInstPerRow();
		
		String instCol_R = "instCol" + Utility.getRandomString(8);
		sb.append(instCol_R + "<- \"" + this.instanceColumn + "\";");
		String attrList_R = "attrList" + Utility.getRandomString(8);
		sb.append(attrList_R + "<- " + RSyntaxHelper.createStringRColVec(this.attributeNamesList.toArray())+ ";");
		int sbLength = sb.length();
		String tempDt_R = "tempDt" + Utility.getRandomString(8);
		if (uniqInstPerRowStr != null && uniqInstPerRowStr.equalsIgnoreCase("TRUE")) {
			sb.append(tempDt_R + " <- " +  dtName + "[complete.cases(" +  dtName + "[[" +  instCol_R + "]]), ];");
		} else {
			sb.append(tempDt_R + " <- " +  dtName + "[complete.cases(" + dtName + "[[" + instCol_R + "]]), "
					+ "lapply(.SD, mean, na.rm=TRUE), by = " + instCol_R + ", .SDcols = " + attrList_R + "];");
		}
		this.rJavaTranslator.runR(sb.toString());
		sb.delete(sbLength, sb.length());
		int nrows = this.rJavaTranslator.getInt("nrow(" + tempDt_R + ");");
		
		String kStr = getK();
		boolean intK = this.rJavaTranslator.getBoolean("all(" + "c(" + kStr + ") == floor( "+ "c(" + kStr + ")))");
		this.kArray = this.rJavaTranslator.getDoubleArray("c(" + kStr + ")");
		if (!intK || this.kArray == null || this.kArray.length == 0){
			this.rJavaTranslator.runR("rm(" + tempDt_R + "," + instCol_R + "," + attrList_R + ");gc();");
			throw new IllegalArgumentException("K must be either a single integer or a list of integers only.");
		}
		for (double k : this.kArray){
			if (k > nrows){
				this.rJavaTranslator.runR("rm(" + tempDt_R + "," + instCol_R + "," + attrList_R + ");gc();");
				throw new IllegalArgumentException("K must be less than the number of unique instances, " + nrows + ".");
			}
		}
		String k_R = "kNeighbors" + Utility.getRandomString(8);
		sb.append(k_R + "<- c(" + kStr + ");");
		
		// LOF r script
		String RLOFScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\LOF.R";
		RLOFScriptFilePath = RLOFScriptFilePath.replace("\\", "/");
		sb.append("source(\"" + RLOFScriptFilePath + "\");");
				
		// set call to R function
		sb.append(dtName + " <- runLOF( " + dtName + "," + tempDt_R + "," + instCol_R + "," + attrList_R + "," + k_R + ");");
				
		// execute R
		this.rJavaTranslator.runR(sb.toString());

		// retrieve new columns to add to meta
		String[] updatedDfColumns = this.rJavaTranslator.getColumns(dtName);

		// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + tempDt_R + "," + instCol_R + "," + attrList_R + "," + k_R + ",runLOF, getLOP, getNewColumnName, normalizeCol);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());

		Collection<String> origDfCols = new ArrayList<String>(Arrays.asList(frame.getColumnHeaders()));
		Collection<String> updatedDfCols = new ArrayList<String>(Arrays.asList(updatedDfColumns));
		updatedDfCols.removeAll(origDfCols);
		if (!updatedDfCols.isEmpty()) {
			for (String newColName : updatedDfCols) {
				meta.addProperty(dtName, dtName + "__" + newColName);
				meta.setAliasToProperty(dtName + "__" + newColName, newColName);
				meta.setDataTypeToProperty(dtName + "__" + newColName, "DOUBLE");
			}
		} else {
			// no results
			throw new IllegalArgumentException("LOF algorithm returned no results.");
		}

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	////////////////////// Input Methods///////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	private String getInstanceColumn() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[0]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}

		// else, we assume it is the first column
		if (this.curRow == null || this.curRow.size() == 0) {
			String errorString = "Could not find the instance column";
			throw new IllegalArgumentException(errorString);
		}
		return this.curRow.get(0).toString();
	}

	private String getUniqInstPerRow() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(UNIQUE_INSTANCE_PER_ROW);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString().toLowerCase();
			}
		}
		return null;
	}
	
	private String getK() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(K_NEIGHBORS);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString().replaceAll("\\s", "");
			}
		} else {
			throw new IllegalArgumentException("Please specify kNeighbors.");
		}
		return null;
	}
	
	private List<String> getAttrList() {
		List<String> retList = new ArrayList<String>();
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[1]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				retList.add(noun.getValue().toString());
			}
		} else {
			throw new IllegalArgumentException("Please specify attributes.");
		}
		return retList;
	}
}
