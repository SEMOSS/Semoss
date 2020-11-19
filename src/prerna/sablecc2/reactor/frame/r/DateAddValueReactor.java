package prerna.sablecc2.reactor.frame.r;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateAddValueReactor extends AbstractRFrameReactor{

	/*
	 * Here are the keys that can be passed into the reactor options
	 */
	private static final String UNIT = "unit";
	private static final String VAL_TO_ADD = "val_to_add";
	private static final String NEW_COL = "new_col";
	
	/*
	 * Here are the units that can be used 
	 */
	private static final String DAY = "day";
	private static final String WEEK = "week";
	private static final String MONTH = "month";
	private static final String YEAR = "year";
	
	public DateAddValueReactor(){
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), NEW_COL, UNIT, VAL_TO_ADD};
	}
	
	@Override
	public NounMetadata execute() {
		// Initiate R
		init();
		organizeKeys();

		RDataTable frame = (RDataTable) getFrame();
		String table = frame.getName();

		String srcCol = this.keyValue.get(this.keysToGet[0]);
		String newCol = this.keyValue.get(this.keysToGet[1]);
		String unit = this.keyValue.get(this.keysToGet[2]).toLowerCase();
		int value = getValue();
		
		// make sure source column exists
		String[] startingColumns = getColumns(table);
		List<String> startingColumnsList = new Vector<String>(startingColumns.length);
		startingColumnsList.addAll(Arrays.asList(startingColumns));
		if (srcCol == null || !startingColumnsList.contains(srcCol)) {
			throw new IllegalArgumentException("Need to define an existing date column.");
		}
		
		StringBuilder script = new StringBuilder();
		if(newCol != null && !newCol.isEmpty()){
			newCol = getCleanNewColName(frame, newCol);
			script.append(table).append("$").append(newCol).append(" <- ").append(table).append("$").append(srcCol);
		} else{
			script.append(table).append("$").append(srcCol).append(" <- ").append(table).append("$").append(srcCol);
		}
		if(unit.equals(DAY)){
			script.append(" + ").append(value).append(";");
		} else if(unit.equals(WEEK)){
			script.append(" + ").append(value * 7).append(";");
		} else if(unit.equals(MONTH)){
			script.append(" %m+%").append(" months(").append(value).append(")").append(";");
		} else if(unit.equals(YEAR)){
			script.append("%m+%").append(" years(").append(value).append(")").append(";");
		}
		this.rJavaTranslator.runR(script.toString());
		
		NounMetadata retNoun;
		if(newCol == null || newCol.equals("")){
			retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		} else{
			retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
			// get src column data type
			String addedColumnDataType = SemossDataType.DATE.toString();
			OwlTemporalEngineMeta metaData = frame.getMetaData();
			metaData.addProperty(table, table + "__" + newCol);
			metaData.setAliasToProperty(table + "__" + newCol, newCol);
			metaData.setDataTypeToProperty(table + "__" + newCol, addedColumnDataType);
			metaData.setDerivedToProperty(table + "__" + newCol, true);
			frame.syncHeaders();
		}
		return retNoun;
	}
	
	private int getValue() {
		GenRowStruct grs = this.store.getNoun(VAL_TO_ADD);
		if(grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Missing Necessary Value to Run");
		}
		NounMetadata noun = grs.getNoun(0);
		if(noun.getNounType() == PixelDataType.CONST_INT) {
			return (int) grs.get(0);
		}
		throw new IllegalArgumentException("Missing Necessary Value to Run");
	}
}
