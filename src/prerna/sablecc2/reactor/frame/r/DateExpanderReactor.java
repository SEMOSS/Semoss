package prerna.sablecc2.reactor.frame.r;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateExpanderReactor extends AbstractRFrameReactor {

	/*
	 * Here are the keys that can be passed into the reactor options
	 */
	
	private static final String YEAR = "year";
	private static final String MONTH = "month";
	private static final String MONTH_NAME = "month-name";
	private static final String DAY = "day";
	private static final String WEEKDAY = "weekday";
	
	public DateExpanderReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.OPTIONS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		String table = frame.getName();
		
		String srcCol = this.keyValue.get(this.keysToGet[0]);
		List<String> options = getOptions(this.keysToGet[1]);
		// make sure source column exists
		String[] startingColumns = getColumns(table);
		List<String> startingColumnsList = new Vector<String>(startingColumns.length);
		startingColumnsList.addAll(Arrays.asList(startingColumns));
		if (srcCol == null || !startingColumnsList.contains(srcCol)) {
			throw new IllegalArgumentException("Need to define an existing date column.");
		}
		
		// create and run script
		int numColumnsToAdd = options.size();
		List<String> addedColumnNames = new Vector<String>(numColumnsToAdd);
		Map<String, String> addedColumnDataType = new HashMap<String, String>();
		// also re add the options again for a list where we will remove to make sure they were properly added
		List<String> addedColumnOptions = new Vector<String>(numColumnsToAdd);
		addedColumnOptions.addAll(options);
		StringBuilder script = new StringBuilder();
		for(int i = 0; i < numColumnsToAdd; i++){
			String newColName = srcCol + "_" + options.get(i);
			newColName = getCleanNewColName(table, newColName);
			addedColumnNames.add(newColName);
			if(options.get(i).equalsIgnoreCase(YEAR)){
				script.append(table).append("$").append(newColName).append(" <- format( ").append(table).append("$").append(srcCol).append(", \"%Y\");");
				addedColumnDataType.put(newColName, SemossDataType.INT.toString());
			}
			else if(options.get(i).equalsIgnoreCase(MONTH)){
				script.append(table).append("$").append(newColName).append(" <- format( ").append(table).append("$").append(srcCol).append(", \"%m\");");
				addedColumnDataType.put(newColName, SemossDataType.INT.toString());
			}
			else if(options.get(i).equalsIgnoreCase(MONTH_NAME)){
				script.append(table).append("$").append(newColName).append(" <- format( ").append(table).append("$").append(srcCol).append(", \"%B\");");
				addedColumnDataType.put(newColName, SemossDataType.STRING.toString());
			}
			else if(options.get(i).equalsIgnoreCase(DAY)){
				script.append(table).append("$").append(newColName).append(" <- format( ").append(table).append("$").append(srcCol).append(", \"%d\");");
				addedColumnDataType.put(newColName, SemossDataType.INT.toString());
			}
			else if(options.get(i).equalsIgnoreCase(WEEKDAY)){
				script.append(table).append("$").append(newColName).append(" <- format( ").append(table).append("$").append(srcCol).append(", \"%A\");");
				addedColumnDataType.put(newColName, SemossDataType.STRING.toString());
			}
		}
		this.rJavaTranslator.runR(script.toString());
		
		// check to make sure columns are actually in the frame	
		// if nothing added
		// throw illegal argument exception
		List<String> endColumnsList = new Vector<String>(startingColumns.length + numColumnsToAdd);
		endColumnsList.addAll(Arrays.asList(this.rJavaTranslator.getColumns(table)));
		// remove all the column names from the table from the ones we added
		// if it is empty, all are added
		// if the size is the same as at the start, none were added
		// otherwise some were added while others were not
		List<String> operationsNotAdded = new Vector<String>();
		for(int i = 0; i < numColumnsToAdd; i++) {
			if(!endColumnsList.contains(addedColumnNames.get(i))) {
				operationsNotAdded.add(options.get(i));
			}
		}

		NounMetadata warning = null;
		if(operationsNotAdded.size() == numColumnsToAdd){
			throw new IllegalArgumentException("No new columns were added.");
		} else if(!operationsNotAdded.isEmpty()) {
			warning = NounMetadata.getWarningNounMessage("The following operations were not appended: " + options.toString());
		}

		// get src column data type
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		// update meta data only for the columns added
		endColumnsList.removeAll(startingColumnsList);
		for(int i = 0; i < endColumnsList.size(); i++){
			String newColName = endColumnsList.get(i);
			metaData.addProperty(table, table + "__" + newColName);
			metaData.setAliasToProperty(table + "__" + newColName, newColName);
			metaData.setDataTypeToProperty(table + "__" + newColName, addedColumnDataType.get(newColName));
			metaData.setDerivedToProperty(table + "__" + newColName, true);
		}
		frame.syncHeaders();
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		if(warning != null){
			retNoun.addAdditionalReturn(warning);
		} else {
			retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully extracted details from date field"));
		}
		
		return retNoun;
	}
	
	// returns a list of the date extraction types (options) desired by the user
	private List<String> getOptions(String key) {
		// instantiate var ruleList as a list of strings 
		List<String> optionList = new Vector<String>();
		// Class call to make grs to get the Noun of getRules
		GenRowStruct grs = this.store.getNoun(key);

		if(grs == null || grs.isEmpty()) {
			// add all the operations
			optionList.add(YEAR);
			optionList.add(MONTH);
			optionList.add(MONTH_NAME);
			optionList.add(DAY);
			optionList.add(WEEKDAY);
			return optionList;
		}
		// Assign size to the length of grs
		int size = grs.size();
		// Iterate through the rule and add the value to the list
		for(int i = 0; i < size; i++) {
			optionList.add(grs.get(i).toString());
		}
		return optionList;
	}
	
}
