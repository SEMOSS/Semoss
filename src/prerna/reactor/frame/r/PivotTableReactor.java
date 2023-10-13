package prerna.reactor.frame.r;

import java.util.List;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class PivotTableReactor extends AbstractRFrameReactor {

	/**
	 * This reactor creates a pivot table based on various thing
	 * Row Groups - what do you want your row to be - basically columns
	 * Column Groups - What are the columns on the pivot table
	 * Values - The calculation columns that is needed
	 * Following are the calculations possible
	 * Sum
	 * mean
	 * max
	 * min
	 * n() - just count
	 * standard deviation - sd
	 * ignoring na - max(SchedSpeedMPH, na.rm=TRUE) - this should be a checkbox
	 * 
	 */
	
	public PivotTableReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ROW_GROUPS.getKey(), ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.VALUES.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		// so this is going to come in as vectors
		List rowGroups = this.store.getNoun(keysToGet[0]).getAllValues();
		List colGroups = this.store.getNoun(keysToGet[1]).getAllValues();
		List values = this.store.getNoun(keysToGet[2]).getAllValues();

		//-	pt <- qpvt(df, c("gender", "frame"), c("location"), calculations=c("TOTAL Chol" = "sum(chol)", "TOTAL Age" = "sum(age)"))
		
		// get the frame
		RDataTable frame = (RDataTable) getFrame();
		// get frame name
		String table = frame.getName();

		// convert the inputs into a cgroup
		StringBuilder rows = new StringBuilder("c(");
		for(int rowIndex = 0;rowIndex < rowGroups.size();rowIndex++)
		{
			if(rowIndex > 0)
				rows.append(",");
			rows.append("\"").append(rowGroups.get(rowIndex)).append("\"");
		}
		rows.append(")");
		
		// convert columns next
		StringBuilder cols = new StringBuilder("c(");
		for(int colIndex = 0;colIndex < colGroups.size();colIndex++)
		{
			if(colIndex > 0)
				cols.append(",");
			cols.append("\"").append(colGroups.get(colIndex)).append("\"");
		}
		cols.append(")");
		
		// last piece is the calculations
		// not putting headers right now
		StringBuilder calcs = new StringBuilder("c(");
		for(int calcIndex = 0;calcIndex < values.size();calcIndex++)
		{
			if(calcIndex > 0)
				calcs.append(",");
			calcs.append("\"").append(values.get(calcIndex)).append("\"");
		}
		calcs.append(")");
		
		String pivotName = "pivot" + Utility.getRandomString(5);
		String htmlName = pivotName + ".html";
		
		String genPivot = pivotName + " <- qpvt(" + table + "," + rows + "," + cols + "," + calcs + ")";
		// create the html
		genPivot = genPivot + "; " + pivotName + "$saveHtml(paste(ROOT" + ",\"/" + htmlName +"\", sep=\"\"))";
		
		// delete the pivot later
		
		this.rJavaTranslator.runRAndReturnOutput(genPivot);
		
		
/*		organizeKeys();
		// initialize rJavaTranslator
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		// get frame name
		String table = frame.getName();
		// get inputs
		String column = this.keyValue.get(this.keysToGet[0]);
		if (column == null) {
			column = getExistingColumn();
		}
		// clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		String regexToCount = this.keyValue.get(this.keysToGet[1]);
		if (regexToCount == null) {
			regexToCount = getRegex();
		}
		String newColName = this.keyValue.get(this.keysToGet[2]);
		if (newColName == null) {
			newColName = getNewColumn();
		}
		// check if new colName is valid
		newColName = getCleanNewColName(frame, newColName);

		// this function only works on strings, so we must convert the data to a
		// string if it is not already
		String colType = this.rJavaTranslator.getColumnType(table, column);
		if (colType.equalsIgnoreCase("numeric") || colType.equalsIgnoreCase("date")) {
			// after performing the count function, we will change it back
			// format numeric data to get rid of e format (1e6)
			// df$MovieBudget <- as.character(df$MovieBudget);
			StringBuilder rsb = new StringBuilder();
			rsb.append(table + "$" + column + " <- as.character(format(" + table + "$" + column + ",scientific=FALSE));");
			// count
			rsb.append(table + "$" + newColName + " <- str_count(" + table + "$" + column + ", " + "\"" + regexToCount + "\"" + ");");
			// df$MovieBudget <- as.numeric(df$col);
			rsb.append(table + "$" + column + "<- as.numeric(" + table + "$" + column + ");");
			this.rJavaTranslator.runR(rsb.toString());
		} else {
			// define script to be executed
			// dt$new <- str_count(dt$oldCol, "strToFind");
			String script = table + "$" + newColName + " <- str_count(" + table + "$" + column + ", " + "\"" + regexToCount + "\"" + ")";
			// execute the script
			frame.executeRScript(script);
		}

		// update the metadata
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(table, table + "__" + newColName);
		metaData.setAliasToProperty(table + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(table + "__" + newColName, SemossDataType.DOUBLE.toString());
		this.getFrame().syncHeaders();
*/
		// NEW TRACKING
		/*UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"CountIf", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		*/
		
		return new NounMetadata("ROOT/" + htmlName, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}




	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////


	private String getExistingColumn() {
		// first input is the name of the column
		// that the operation is being done on
		NounMetadata noun = this.curRow.getNoun(0);
		String column = noun.getValue().toString();
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		return column;
	}

	private String getRegex() {
		// second input is the regex to count
		NounMetadata noun = this.curRow.getNoun(1);
		return noun.getValue().toString();
	}

	private String getNewColumn() {
		// third input is the new column name
		NounMetadata noun = this.curRow.getNoun(2);
		String column = noun.getValue().toString();
		return column;
	}
	
}
