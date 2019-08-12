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

public class DateDifferenceReactor extends AbstractRFrameReactor {

	/*
	 * Here are the keys that can be passed into the reactor options
	 */
	
	private static final String INPUT_DATE = "input_date";
	private static final String INPUT_AS_START = "input_as_start";
	private static final String UNIT = "unit";
	
	public DateDifferenceReactor(){
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey(), INPUT_DATE, UNIT, INPUT_AS_START, ReactorKeysEnum.NEW_COLUMN.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		String table = frame.getName();
				
		List<String> srcCols = getColumns();
		String inputDate = this.keyValue.get(this.keysToGet[1]);
		String unit = this.keyValue.get(this.keysToGet[2]);
		String inputAsStart = this.keyValue.get(this.keysToGet[3]);
		String newColName = this.keyValue.get(this.keysToGet[4]);
				
		newColName = getCleanNewColName(frame, table, newColName);
		
		// if passing 2 cols or passing a static date val
		Boolean twoColumns = false;
		if(inputDate.equals("")){
			twoColumns = true;
		}
		
		// make sure columns are in list and the proper inputs are given
		String[] startingColumns = getColumns(table);
		List<String> startingColumnsList = new Vector<String>(startingColumns.length);
		startingColumnsList.addAll(Arrays.asList(startingColumns));
		int numCols = srcCols.size();
		String col;
		if(numCols == 0){
			throw new IllegalArgumentException("Need to define at least one date column.");
		}
		else if(numCols == 1){
			col = srcCols.get(0);
			if(!startingColumnsList.contains(col) || inputDate.equals("")){
				throw new IllegalArgumentException("Need to define two existing date columns or an existing date column and a static date.");
			}
		}
		else if(numCols == 2){
			for(int i = 0; i < numCols; i++){
				if(!startingColumnsList.contains(srcCols.get(i))){
					throw new IllegalArgumentException("Need to define two existing date columns or an existing date column and a static date.");
				}
			}
		}
		else if(numCols > 2 || (numCols >= 2 && !inputDate.equals(""))){
			throw new IllegalArgumentException("Need to define ONLY two existing date columns OR an existing date column and a static date.");
		}
		
		
		// create and run script
		StringBuilder script = new StringBuilder();
		String addedColumnDataType = SemossDataType.INT.toString();
		if(twoColumns) {
			script.append(table).append("$").append(newColName).append(" <- round(as.numeric(difftime(").append(table).append("$").append(srcCols.get(1)).append(", ");
			script.append(table).append("$").append(srcCols.get(0));
		}
		else{
			if(inputAsStart.equals("true")) {
				script.append(table).append("$").append(newColName).append(" <- round(as.numeric(difftime(").append(table).append("$");
				script.append(srcCols.get(0)).append(", as.Date(\"").append(inputDate).append("\"),");
			} else {
				script.append(table).append("$").append(newColName).append(" <- round(as.numeric(difftime(").append("as.Date(\"").append(inputDate).append("\"), ");
				script.append(table).append("$").append(srcCols.get(0));
			}
		}
		
		// append different things for different units
		if(unit.equals("weeks") || unit.equals("days")) {
			script.append(", units = \"").append(unit).append("\")), digits = 2);");
		}
		else if(unit.equals("years")) {
			script.append(", units = \"days\"))/365, digits = 2);");
		}
		else if(unit.equals("months")) {
			//using 365/12 as month time
			script.append(", units = \"days\"))/30.42, digits = 2);");
		}
		this.rJavaTranslator.runR(script.toString());
		
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
	
	
	private List<String> getColumns() {
		List<String> columns = new Vector<>();
		// get columns by key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		NounMetadata noun;
		if (grs != null) {
			for (int i = 0; i < grs.size(); i++) {
				noun = grs.getNoun(i);
				if (noun != null) {
					String column = noun.getValue() + "";
					if (column.length() > 0) {
						columns.add(column);
					}
				}
			}
		} else {
			// get columns by index
			int inputSize = this.getCurRow().size();
			for (int i = 2; i < inputSize; i++) {
				NounMetadata input = this.getCurRow().getNoun(i);
				String column = input.getValue() + "";
				columns.add(column);
			}
		}
		return columns;
	}
	
}

