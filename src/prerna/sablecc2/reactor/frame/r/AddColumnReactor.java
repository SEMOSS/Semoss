package prerna.sablecc2.reactor.frame.r;

import java.util.Arrays;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AddColumnReactor extends AbstractRFrameReactor {

	/**
	 * This reactor adds an empty column to the frame The inputs to the reactor are: 
	 * 1) the name for the new column 
	 * 2) the new column type
	 */
	
	public AddColumnReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.NEW_COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(), ReactorKeysEnum.COLUMN.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		// get new column name from the gen row struct
		String newColName = this.keyValue.get(this.keysToGet[0]);
		
		// get the column type and standardize
		String colType = this.keyValue.get(this.keysToGet[1]);
		if (colType == null){
			colType = SemossDataType.convertStringToDataType("STRING").toString();
		}
		
		if (newColName == null || newColName.isEmpty()){
			throw new IllegalArgumentException("Need to define the new column name");
		}

		String table = frame.getTableName();
		// clean colName
		if (newColName.contains("__")) {
			String[] split = newColName.split("__");
			newColName = split[1];
		}
		// clean the column name to ensure that it is valid
		newColName = getCleanNewColName(table, newColName);

		// define the script to be executed;
		// this assigns a new column name with no data in columns
		String script = table + "$" + newColName + " <- \"\" ";
		// execute the r script
		frame.executeRScript(script);

		// update the metadata to include this new column
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(table, table + "__" + newColName);
		metaData.setAliasToProperty(table + "__" + newColName, newColName);
		
		// copy values from column to new column
		String duplicateSrcCol = this.keyValue.get(this.keysToGet[2]).trim();
		if (duplicateSrcCol != null && !duplicateSrcCol.isEmpty()){
			// make sure src column to duplicate exists, if not it will just be blank
			String[] allCol = getColumns(table);
			if (Arrays.asList(allCol).contains(duplicateSrcCol)) {
				String duplicate = table + "$" + newColName + "<-" + table + "$" + duplicateSrcCol + ";";
				frame.executeRScript(duplicate);
			}
		}

		// temp table used to assign a data type to the new column
		String tempTable = null;
		if (Utility.isNumericType(colType)) {
			// update the metadata depending on the data type
			metaData.setDataTypeToProperty(table + "__" + newColName, "NUMBER");
			tempTable = Utility.getRandomString(6);
			script = tempTable + " <- as.numeric(" + table + "$" + newColName + ")";
			frame.executeRScript(script);
			script = table + "$" + newColName + "<-" + tempTable;
			frame.executeRScript(script);
		} else if (Utility.isDateType(colType)) {
			metaData.setDataTypeToProperty(table + "__" + newColName, "DATE");
			tempTable = Utility.getRandomString(6);
			String dateFormat = "%Y/%m/%d";
			script = tempTable + " <- as.Date(" + table + "$" + newColName + ", format='" + dateFormat + "')";
			frame.executeRScript(script);
			script = table + "$" + newColName + " <- " + tempTable;
			frame.executeRScript(script);
		} else {
			// if not a number or a date then assign to string
			metaData.setDataTypeToProperty(table + "__" + newColName, "STRING");
		}
		

		// r temp variable clean up
		if (tempTable != null) {
			frame.executeRScript("rm(" + tempTable + ");");
			frame.executeRScript("gc();");
			this.getFrame().syncHeaders();
		}
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColName));
		return retNoun;
	}
}
