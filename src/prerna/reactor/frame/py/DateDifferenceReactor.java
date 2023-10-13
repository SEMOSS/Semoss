package prerna.reactor.frame.py;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateDifferenceReactor extends AbstractPyFrameReactor {

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
		organizeKeys();
		
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		String table = frame.getName();
		String wrapperName = frame.getWrapperName();

		String startCol = this.keyValue.get(this.keysToGet[0]);
		String endCol = this.keyValue.get(this.keysToGet[1]);
		String inputUse = this.keyValue.get(this.keysToGet[2]);
		String inputDate = this.keyValue.get(this.keysToGet[3]);
		String unit = this.keyValue.get(this.keysToGet[4]).toLowerCase();
		String newColName = this.keyValue.get(this.keysToGet[5]);
				
		newColName = getCleanNewColName(frame, newColName);
		
		// make sure columns are in list and the proper inputs are given
		String[] startingColumns = getColumns(frame);
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
			script.append(wrapperName).append(".date_difference_columns(")
				.append(endCol).append(",")
				.append(startCol).append(",")
				.append(unit).append(",")
				.append(newColName)
				;
		} else if(inputUse.equals("start")) {
			script.append(wrapperName).append(".date_difference_constant(")
				.append(endCol).append(",")
				.append(inputDate).append(",")
				.append("True,")
				.append(unit).append(",")
				.append(newColName)
				;
		} else if(inputUse.equals("end")) {
			script.append(wrapperName).append(".date_difference_constant(")
				.append(startCol).append(",")
				.append(inputDate).append(",")
				.append("False,")
				.append(unit).append(",")
				.append(newColName)
				;
		}
		
		frame.runScript(script.toString());
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

