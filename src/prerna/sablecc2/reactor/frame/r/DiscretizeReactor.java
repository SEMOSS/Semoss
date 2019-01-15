package prerna.sablecc2.reactor.frame.r;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class DiscretizeReactor extends AbstractRFrameReactor {
	/**
	 * Discretize([{"column":"PetalLength"}, {"column":"SepalLength", "breaks":"(4.3, 5.5, 6.7, 7.9)", "labels":"(Short,Medium,Long)"},
	 * 			   {"column":"PetalWidth", "breaks":"0:5*.5"}])
	 * Discretize({"column":"MovieBudget", "numDigits":"10"}) 
	 * Input keys: 
	 * 		1. column (required) 
	 * 		2. breaks (conditionally required - req only if labels specified) - can be one of 3 types: integer, breakpoints as a list,
	 * 			mathematical notation of range - breakpoints as a list or mathematical notation of range need to be specified in 
	 * 			ascending order 
	 * 		3. labels (optional)
	 * 		4. numDigits (optional) specifies number of digits used in formatting the break/range numbers
	 * 
	 * Return format: if labels is not specified, then the discretized ranges will be wrapped with [ ] (inclusive) or () or a combination 
	 * and contain the lower and upper range, comma separated: ex. [0, 100) aka 0 =< x < 100
	 * 
	 */
	private static final String requestMap = "requestMap";

	public DiscretizeReactor() {
		this.keysToGet = new String[] { requestMap };
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] { "data.table", "dplyr" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();

		String dtName = frame.getName();
		List<String> colNames = Arrays.asList(frame.getColumnNames());
		List<Object> reqList = this.curRow.getValuesOfType(PixelDataType.MAP);

		StringBuilder inputListSB = new StringBuilder();
		for (int i = 0; i < reqList.size(); i++) {
			StringBuilder listSB = new StringBuilder();
			Map<String, Object> parsedMap = (Map<String, Object>) reqList.get(i);
			String name = (String) parsedMap.get("column");
			if (name == null || name == "") {
				throw new IllegalArgumentException("Column name needs to be specified.");
			} else if (!colNames.contains(name)) {
				throw new IllegalArgumentException(
						"Specified column name, " + name + ", is unavailable in the data frame.");
			}

			// if we get to this point, we have a valid column name specified
			listSB.append(name + "=");

			// get column data type & only proceed if type = numeric
			String dataType = meta.getHeaderTypeAsString(dtName + "__" + name);
			if (Utility.isNumericType(dataType)) {
				String breaks = (String) parsedMap.get("breaks");
				String labels = (String) parsedMap.get("labels");
				String numDigitsStr = (String) parsedMap.get("numDigits");

				// validate that if breaks specified, then it doesn't contain
				// any alpahbetical characters
				if (breaks == null || breaks == "") {
					// breaks var was not specified
					listSB.append("list(");
				} else {
					breaks = breaks.replaceAll("[()]", "").trim();
					if (breaks != null && breaks != "" && breaks.matches(".*[a-zA-z]+.*") == true) {
						throw new IllegalArgumentException("Breaks should be either a numerical integer or a "
								+ "numerical vector. No alphabetical characters allowed.");
					} else {
						// valid breaks specified
						if ((Object) breaks instanceof Integer) {
							listSB.append("list(breaks=" + breaks + ")");
						} else {
							listSB.append("list(breaks=c(" + breaks + ")");
						}
					}
				}

				// validate that if labels specified, then valid breaks variable
				// is available also
				boolean isValidLabels = false;
				if (labels != null && labels != "") {
					if (breaks == null || breaks == "" || breaks.matches(".*[a-zA-z]+.*") == true) {
						throw new IllegalArgumentException("Please specify breaks (cannot contain "
								+ "alphabetical characters) - breaks are required if labels are provided.");
					} else {
						// check if labels contains whitespaces, then replace with
						// underscore
						labels = Utility.decodeURIComponent((String) labels).replaceAll("[()]", "").trim();
						String[] labelsSplit = labels.split(",");
						List<String> labelsList = Arrays.asList(labelsSplit);
						if (labelsList.size() < 2)
							throw new IllegalArgumentException("Labels has to contain more than 1 itme");
						for (int j = 0; j < labelsList.size(); j++) {
							String jLabel = "'" + labelsList.get(j).replaceAll("\"", "").trim().replaceAll("\\s", "_")
									+ "'";
							labelsList.set(j, jLabel);
						}
						labels = String.join(",", labelsList);
						listSB.append(", labels=c(" + labels + ")");
						isValidLabels = true;
					}
				}
				
				// validate that if numDigits specified AND labels is absent, then numDigits is a positive integer > 0
				if (numDigitsStr == null || numDigitsStr == "" || isValidLabels == true) {
					listSB.append(")");
				} else {
					try {
						int numDigits = Integer.parseInt(numDigitsStr);
						if (numDigitsStr.replaceAll("[\\D]", "").matches("^[0-9]*[1-9][0-9]*$")) {
							if (listSB.indexOf("(") == listSB.length() - 1) {
								listSB.append("dig.lab=" + numDigits + ")");
							} else {
								listSB.append(", dig.lab=" + numDigits + ")");
							}
						} else {
							throw new IllegalArgumentException("Number of digits specified must be a positive integer.");
						}
					} catch (NumberFormatException e) {
						throw new IllegalArgumentException("Number of digits specified must be an integer.");
					}
				}
			}

			if (listSB.length() > 0) {
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

		// discretize r scripts
		for (String fileName : new String[] { "Discretize_Source.R", "Discretize.R" }) {
			String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\" + fileName;
			scriptFilePath = scriptFilePath.replace("\\", "/");
			sb.append("source(\"" + scriptFilePath + "\");");
		}

		// set call to R function
		sb.append(dtName + " <- discretizeColumnsDt( " + dtName + "," + inputList_R + ");");

		// execute R
		this.rJavaTranslator.runR(sb.toString());

		// retrieve new columns to add to meta
		List<String> updatedDtColumns = Arrays.asList(this.rJavaTranslator.getColumns(dtName));
		List<String> updatedDtColsSubset = new ArrayList<String>(CollectionUtils.removeAll(updatedDtColumns, colNames));

		String colLevels_R = "colLevels" + Utility.getRandomString(8);
		if (!updatedDtColsSubset.isEmpty()) {
			for (String newColName : updatedDtColsSubset) {
				meta.addProperty(dtName, dtName + "__" + newColName);
				meta.setAliasToProperty(dtName + "__" + newColName, newColName);
				meta.setDataTypeToProperty(dtName + "__" + newColName, "FACTOR");
				this.rJavaTranslator.runR(new StringBuilder()
						.append(colLevels_R + "<-" + RSyntaxHelper.getOrderedLevelsFromRFactorCol(dtName, newColName))
						.toString());
				String orderedLevels = this.rJavaTranslator.getString(colLevels_R);
				meta.setOrderingToProperty(dtName + "__" + newColName, orderedLevels);
			}
		} else {
			// no results
			throw new IllegalArgumentException("The selected columns could not be discretized.");
		}

		// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + inputList_R + "," + colLevels_R + ",discretizeColumnsDt, discretize, getNewColumnName);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"Discretize", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
