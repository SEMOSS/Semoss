package prerna.reactor.frame.py;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class DiscretizeReactor extends AbstractPyFrameReactor {
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
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		String dtName = frame.getName();

		// get wrapper name
		String wrapperFrameName = frame.getWrapperName();
		
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();		
		List<String> colNames = Arrays.asList(frame.getColumnHeaders());  // check if this is the same as R's frame.getColumnNames()
		List<Object> reqList = this.curRow.getValuesOfType(PixelDataType.MAP);

//		StringBuilder inputListSB = new StringBuilder();
		for (int i = 0; i < reqList.size(); i++) {
			StringBuilder listSB = new StringBuilder();
			Map<String, Object> parsedMap = (Map<String, Object>) reqList.get(i);
			String name = (String) parsedMap.get("column");
			if (name == null || (name = name.trim()).isEmpty()) {
				throw new IllegalArgumentException("Column name needs to be specified.");
			} else if (!colNames.contains(name)) {
				throw new IllegalArgumentException("Specified column name, " + name + ", is unavailable in the data frame.");
			}
			
			// if we get to this point, we have a valid column name specified
			
			// get column data type & only proceed if type = numeric
			String dataType = meta.getHeaderTypeAsString(dtName + "__" + name);
			
			if (!Utility.isNumericType(dataType)) {
				throw new IllegalArgumentException("Specified column name, " + name + ", must be a numeric type");
			}
			
			String newColName = getCleanNewColName(frame, name);
			listSB.append(wrapperFrameName + ".discretize_column('" + name + "', '" + newColName + "'");
			
			String breaks = (String) parsedMap.get("breaks");
			String labels = (String) parsedMap.get("labels");
			String numDigitsStr = (String) parsedMap.get("numDigits");

			// validate that if breaks specified, then it doesn't contain
			// any alpahbetical characters
			if (breaks == null || breaks.isEmpty()) {
				// breaks var was not specified
			} else {
				breaks = breaks.replaceAll("[()]", "").trim();
				if (breaks != null && !breaks.isEmpty() && breaks.matches(".*[a-zA-z]+.*") == true) {
					throw new IllegalArgumentException("Breaks should be either a numerical integer or a "
							+ "numerical vector. No alphabetical characters allowed.");
				} else {
					// valid breaks specified
					listSB.append(", breaks=");
					if ((Object) breaks instanceof Integer) {
						listSB.append(breaks);
					} else {
						listSB.append("[" + breaks + "]");  // TODO check that this prints out correctly in py
					}
				}
			}
			
			// validate that if labels specified, then valid breaks variable
			// is available also
			boolean isValidLabels = false;
			if (labels != null && !labels.isEmpty()) {
				if (breaks == null || breaks.isEmpty() || breaks.matches(".*[a-zA-z]+.*") == true) {
					throw new IllegalArgumentException("Please specify breaks (cannot contain "
							+ "alphabetical characters) - breaks are required if labels are provided.");
				} else {
					// check if labels contains whitespaces, then replace with underscore
					labels = Utility.decodeURIComponent((String) labels).replaceAll("[()]", "").trim();
					String[] labelsSplit = labels.split(",");
					List<String> labelsList = Arrays.asList(labelsSplit);
					for (int j = 0; j < labelsList.size(); j++) {
						String jLabel = "'" + labelsList.get(j).replaceAll("\"", "").trim().replaceAll("\\s", "_") + "'";
						labelsList.set(j, jLabel);
					}
					labels = String.join(",", labelsList);
					listSB.append(", labels=[" + labels + "]");
					isValidLabels = true;
				}
			}
			
			// validate that if numDigits specified AND labels is absent, then numDigits is a positive integer > 0
			if (numDigitsStr == null || numDigitsStr.isEmpty() || isValidLabels == true) {
			} else {
				try {
					int numDigits = Integer.parseInt(numDigitsStr);
					if (numDigitsStr.replaceAll("[\\D]", "").matches("^[0-9]*[1-9][0-9]*$")) {
						listSB.append(", num_digits=" + numDigits);
					} else {
						throw new IllegalArgumentException("Number of digits specified must be a positive integer.");
					}
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Number of digits specified must be an integer.");
				}
			}
			
			// close-off wrapper method
			listSB.append(")");
			
			// run script
			frame.runScript(listSB.toString());
			this.addExecutedCode(listSB.toString());
		}

		// retrieve new columns to add to meta		
		List<String> updatedDtColumns = new ArrayList<String>(Arrays.asList(getColumns(frame)));
		updatedDtColumns.removeAll(colNames);

		if (!updatedDtColumns.isEmpty()) {
			for (String newColName : updatedDtColumns) {
				meta.addProperty(dtName, dtName + "__" + newColName);
				meta.setAliasToProperty(dtName + "__" + newColName, newColName);
				meta.setDataTypeToProperty(dtName + "__" + newColName, "FACTOR");
				// R's DiscretizeReactor calls getOrderedLevelsFromRFactorCol(), not bothering with that here
			}
		} else {
			// no results
			throw new IllegalArgumentException("The selected columns could not be discretized.");
		}

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"Discretize", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully added discretized column: " + updatedDtColumns.get(0)));
		return noun;
	}
}