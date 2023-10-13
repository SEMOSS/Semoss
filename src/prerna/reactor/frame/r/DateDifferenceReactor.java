package prerna.reactor.frame.r;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateDifferenceReactor extends AbstractRFrameReactor {

	/*
	 * Here are the keys that can be passed into the reactor options
	 */
	private static final String START_COLUMN = "start_column";
	private static final String END_COLUMN = "end_column";
	private static final String INPUT_DATE = "input_date";
	private static final String INPUT_USE = "input_use";
	private static final String UNIT = "unit";
	
	/*
	 * Here are the units that can be used 
	 */
	private static final String DAY = "day";
	private static final String WEEK = "week";
	private static final String MONTH = "month";
	private static final String YEAR = "year";
	
	private static List<String> unitsList = new Vector<String>(4);
	static {
		unitsList.add(DAY);
		unitsList.add(WEEK);
		unitsList.add(MONTH);
		unitsList.add(YEAR);
	}
	
	public DateDifferenceReactor(){
		this.keysToGet = new String[]{START_COLUMN, END_COLUMN, INPUT_USE, INPUT_DATE, UNIT, ReactorKeysEnum.NEW_COLUMN.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		String table = frame.getName();
				
		String startCol = this.keyValue.get(this.keysToGet[0]);
		String endCol = this.keyValue.get(this.keysToGet[1]);
		String inputUse = this.keyValue.get(this.keysToGet[2]);
		String inputDate = this.keyValue.get(this.keysToGet[3]);
		String unit = this.keyValue.get(this.keysToGet[4]);
		String newColName = this.keyValue.get(this.keysToGet[5]);
				
		newColName = getCleanNewColName(frame, newColName);
		
		// make sure columns are in list and the proper inputs are given
		String[] startingColumns = getColumns(table);
		List<String> startingColumnsList = new Vector<String>(startingColumns.length);
		startingColumnsList.addAll(Arrays.asList(startingColumns));
		
		if(!unitsList.contains(unit)){
			throw new IllegalArgumentException("Please pass an appropriate unit value (day, week, month, year).");
		}
		
		if(inputUse.equals("none") || inputUse.equals("")) {
			if(!startingColumnsList.contains(startCol) || !startingColumnsList.contains(endCol))
			throw new IllegalArgumentException("Please pass appropriate parameters.");
		} else if(inputUse.equals("start")){
			if(!startingColumnsList.contains(endCol) || inputDate.equals("")){
				throw new IllegalArgumentException("Please pass appropriate parameters.");
			}
		} else if(inputUse.equals("end")){
			if(!startingColumnsList.contains(startCol) || inputDate.equals("")){
				throw new IllegalArgumentException("Please pass appropriate parameters.");
			}
		}
		
		// create and run script
		StringBuilder script = new StringBuilder();
		String addedColumnDataType =unit.equals(DAY) ? SemossDataType.INT.toString() : SemossDataType.DOUBLE.toString();
		if(inputUse.equals("none") || inputUse.equals("")) {
			script.append(table).append("$").append(newColName).append(" <- round(as.numeric(difftime(").append(table).append("$").append(endCol).append(", ");
			script.append(table).append("$").append(startCol);
		} else if(inputUse.equals("start")) {
			script.append(table).append("$").append(newColName).append(" <- round(as.numeric(difftime(").append(table).append("$");
			script.append(endCol).append(", as.Date(\"").append(inputDate).append("\"),");
		} else if(inputUse.equals("end")) {
			script.append(table).append("$").append(newColName).append(" <- round(as.numeric(difftime(").append("as.Date(\"").append(inputDate).append("\"), ");
			script.append(table).append("$").append(startCol);
		}
		
		// append different things for different units
		if(unit.equals(WEEK) || unit.equals(DAY)) {
			script.append(", units = \"").append(unit).append("\")), digits = 2);");
		}
		else if(unit.equals(YEAR)) {
			script.append(", units = \"days\"))/365, digits = 2);");
		}
		else if(unit.equals(MONTH)) {
			//using 365/12 as month time
			script.append(", units = \"days\"))/30.42, digits = 2);");
		}
		this.rJavaTranslator.runR(script.toString());
		this.addExecutedCode(script.toString());

		// get src column data type
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		metaData.addProperty(table, table + "__" + newColName);
		metaData.setAliasToProperty(table + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(table + "__" + newColName, addedColumnDataType);
		metaData.setDerivedToProperty(table + "__" + newColName, true);
		
		frame.syncHeaders();
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully performed date arithmetic."));
		return retNoun;
	}
	
}

