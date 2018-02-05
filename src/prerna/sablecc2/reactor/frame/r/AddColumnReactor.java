package prerna.sablecc2.reactor.frame.r;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
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
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		// get column name from the gen row struct
		String colName = getNewColumnName();
		// get the column type from the gen row struct
		String colType = getDataType();

		String table = frame.getTableName();
		// clean colName
		if (colName.contains("__")) {
			String[] split = colName.split("__");
			colName = split[1];
		}
		// clean the column name to ensure that it is valid
		colName = getCleanNewColName(table, colName);
		if (colName.equals("")){
			throw new IllegalArgumentException("Provide valid new column name (no special characters)");
		}
		// define the script to be executed;
		// this assigns a new column name with no data in columns
		String script = table + "$" + colName + " <- \"\" ";
		// execute the r script
		frame.executeRScript(script);

		// update the metadata to include this new column
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(table, table + "__" + colName);
		metaData.setAliasToProperty(table + "__" + colName, colName);

		// temp table used to assign a data type to the new column
		String tempTable = null;
		if ((colType.equalsIgnoreCase("number") || colType.equalsIgnoreCase("numeric"))) {
			// update the metadata depending on the data type
			metaData.setDataTypeToProperty(table + "__" + colName, "NUMBER");
			tempTable = Utility.getRandomString(6);
			script = tempTable + " <- as.numeric(" + table + "$" + colName + ")";
			frame.executeRScript(script);
			script = table + "$" + colName + "<-" + tempTable;
		} else if (colType.equalsIgnoreCase("date")) {
			metaData.setDataTypeToProperty(table + "__" + colName, "DATE");
			tempTable = Utility.getRandomString(6);
			String dateFormat = "%Y/%m/%d";
			script = tempTable + " <- as.Date(" + table + "$" + colName + ", format='" + dateFormat + "')";
			frame.executeRScript(script);
			script = table + "$" + colName + " <- " + tempTable;
			frame.executeRScript(script);
		} else {
			// if not a number or a date then assign to string
			metaData.setDataTypeToProperty(table + "__" + colName, "STRING");
		}

		// garbage collection for the temp table
		if (tempTable != null) {
			frame.executeRScript("rm(" + tempTable + ");");
			frame.executeRScript("gc();");
			this.getFrame().syncHeaders();
		}
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(colName));
		return retNoun;
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String getNewColumnName() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// the new column name will be the first input
			String colName = inputsGRS.getNoun(0).getValue() + "";
			if (colName.length() == 0) {
				throw new IllegalArgumentException("Need to define the new column name");
			}
			return colName;
		}
		throw new IllegalArgumentException("Need to define the new column name");
	}

	private String getDataType() {
		// data type defaults to string unless otherwise specified
		String dataType = "STRING";
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS.size() > 1) {
			NounMetadata colTypeNoun = inputsGRS.getNoun(1);
			String dataTypeInput = colTypeNoun.getValue() + "";
			if (dataTypeInput.length() > 0) {
				dataType = dataTypeInput;
			}
		}
		return dataType;
	}
}
