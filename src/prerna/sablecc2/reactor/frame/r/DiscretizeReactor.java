package prerna.sablecc2.reactor.frame.r;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class DiscretizeReactor extends AbstractRFrameReactor {
	/**
	 * Discretize([{"column":"PetalLength"}, {"column":"SepalLength", "breaks":"(4.3, 5.5, 6.7, 7.9)", "labels":"(Short,Medium,Long)"},
	 * {"column":"PetalWidth", "breaks":"0:5*.5"}])
	 * Discretize([{"column":"PetalLength"},{"column":"SepalLength", "breaks":"(4.3, 5.5, 6.7, 7.9)", "labels":"(Short,Medium,Long)", "ordered":"true"}])
	 * Input keys: column (required); breaks (conditionally required - req only if labels specified); labels (optional)
	 */
	private static final String requestMap = "requestMap";
	
	public DiscretizeReactor() {
		this.keysToGet = new String[] {requestMap};
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] { "data.table", "dplyr", "arules" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		
		String dtName = frame.getTableName();
		String[] colNames = frame.getColumnNames();
		List<Object> reqList = this.curRow.getValuesOfType(PixelDataType.MAP);
		List<String> orderedReqCol = new ArrayList<String>();
		
		StringBuilder inputListSB = new StringBuilder();
		for (int i = 0; i < reqList.size(); i++) {
			StringBuilder listSB = new StringBuilder();
			Map<String, Object> parsedMap = (Map<String, Object>) reqList.get(i);
			String name = (String) parsedMap.get("column");
			if (name == null || name == "") {
				throw new IllegalArgumentException("Column name needs to be specified.");
			} else if (ArrayUtilityMethods.arrayContainsValue(colNames, name) ) {
				throw new IllegalArgumentException("Specified column name, " + name + ", is unavailable in the data frame.");
			}
			
			//if we get to this point, we have a valid column name specified
			listSB.append(name + "=");
			
			//get column data type & only proceed if type = numeric
			String dataType = meta.getHeaderTypeAsString(dtName + "__" + name);
			if (Utility.isNumericType(dataType)) {
				String breaks = (String) parsedMap.get("breaks");
				String labels = (String) parsedMap.get("labels");
				Boolean ordered = Boolean.valueOf((String) parsedMap.get("ordered"));
				
				//validate that if breaks specified, then it doesn't contain any alpahbetical characters
				if (breaks == null || breaks == "") {
					//breaks var was not specified
					listSB.append("list(");
				} else {
					breaks = breaks.replaceAll("[()]", "").trim();
					if (breaks != null && breaks != "" && breaks.matches(".*[a-zA-z]+.*") == true) {
						throw new IllegalArgumentException("Breaks should be either a numerical integer or a "
								+ "numerical vector. No alphabetical characters allowed.");
					} else {
						//valid breaks specified
						if ((Object) breaks instanceof Integer) {
							listSB.append("list(breaks=" + breaks + ")");
						} else {
							listSB.append("list(breaks=c(" + breaks + ")");
						}
					}
				}
				
				//validate that if labels specified, then valid breaks variable is available also
				if (labels == null || labels == "") {
					//breaks var was not specified
					listSB.append(")");
				} else if (breaks == null || breaks == "" || breaks.matches(".*[a-zA-z]+.*") == true) {
					throw new IllegalArgumentException("Please specify breaks (cannot contain alphabetical characters) - breaks are required if labels are provided.");
				} else {
					//check if labels contains whitespaces, then replace with underscore
					labels = Utility.decodeURIComponent((String) labels).replaceAll("[()]", "").trim();
					String[] labelsSplit = labels.split(",");
					List<String> labelsList = Arrays.asList(labelsSplit);
					if (labelsList.size() < 2) throw new IllegalArgumentException("Labels has to contain more than 1 itme");
					for (int j = 0; j < labelsList.size(); j++){
						String jLabel = "'" + labelsList.get(j).replaceAll("\"", "").trim().replaceAll("\\s","_") + "'";
						labelsList.set(j, jLabel);
					}
					labels = String.join(",", labelsList);
					if (ordered == true) {
						orderedReqCol.add(name);
						listSB.append(", labels=c(" + labels + "), ordered_result = TRUE)");
					} else {
						listSB.append(", labels=c(" + labels + "))");
					}
				}
			}
			
			if(listSB.length() > 0){
				if (i == 0) {
					inputListSB.append(listSB);
				} else {
					inputListSB.append(", " + listSB);
				}
			}
		}
		
		StringBuilder sb = new StringBuilder();
		String inputList_R = "inputList" + Utility.getRandomString(8);
		sb.append(inputList_R + " <- list(" + inputListSB + ");");
		
		// clustering r script
		String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\Discretize.R";
		scriptFilePath = scriptFilePath.replace("\\", "/");
		sb.append("source(\"" + scriptFilePath + "\");");
		
		// set call to R function
		sb.append(dtName + " <- discretizeColumnsDt( " + dtName + "," + inputList_R + ");");
		
		// execute R
		this.rJavaTranslator.runR(sb.toString());
		
		// add new columns to meta
		// update the metadata to include this new column
		String[] updatedDtColumns = this.rJavaTranslator.getColumns(dtName);
		
		//update frame metadata
		List<String> updatedDtCols = new ArrayList<String>(Arrays.asList(updatedDtColumns));
		updatedDtCols.removeAll(Arrays.asList(colNames));
		String colLevels_R = "colLevels" + Utility.getRandomString(8);
		if (!updatedDtCols.isEmpty()) {
			for (String newColName : updatedDtCols) {
				String nameSplit = newColName.split("_Discretized")[0];
				meta.addProperty(dtName, dtName + "__" + newColName);
				meta.setAliasToProperty(dtName + "__" + newColName, newColName);
//				meta.setDataTypeToProperty(dtName + "__" + newColName, "STRING");
//				meta.setDataTypeToProperty(dtName + "__" + newColName, "FACTOR");
//				if (orderedReqCol.contains(nameSplit)) {
//					this.rJavaTranslator.runR(new StringBuilder().append(colLevels_R + "<-" + 
//							RSyntaxHelper.getOrderedLevelsFromRFactorCol(dtName, newColName)).toString());
//					String orderedLevels = this.rJavaTranslator.getString(colLevels_R);
//					meta.setOrderingToProperty(dtName + "__" + newColName, orderedLevels);
//				}
			}
		} else {
			// no results
			throw new IllegalArgumentException("The selected columns could not be discretized.");
		}
		
		// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + inputList_R + "," + colLevels_R + ",discretizeColumnsDt);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
