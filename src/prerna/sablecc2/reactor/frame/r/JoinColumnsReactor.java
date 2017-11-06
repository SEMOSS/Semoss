package prerna.sablecc2.reactor.frame.r;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class JoinColumnsReactor extends AbstractRFrameReactor {

	/**
	 * This reactor joins columns, and puts the joined string into a new column
	 * with values separated by a separator The inputs to the reactor are: 
	 * 1)
	 * the new column name 
	 * 2) the delimeter 
	 * 3) the columns to join
	 */

	@Override
	public NounMetadata execute() {
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		//get table name
		String table = frame.getTableName();
		
		//create string builder to build the r script
		StringBuilder rsb = new StringBuilder();
		
		//need length of input to use in looping through inputs
		int inputSize = this.getCurRow().size(); 
	
		// first input is what we want to name the new column
		String newColName = getNewColName();
		
		// second input is the delimeter/separator
		String separator = getSeparator();
		
		rsb.append(table + "$" + newColName + " <- paste(");
		
		// the remaining inputs are all of the columns that we want to join
			for (int i = 2; i < inputSize; i++) {
				String column = getColumn(i);
				// separate the column name from the frame name
				if (column.contains("__")) {
					column = column.split("__")[1];
				} 
				
				// continue building the stringbuilder for the r script
				rsb.append(table + "$" + column);
				if (i < inputSize - 1) {
					// add a comma between each column entry
					rsb.append(", ");
				}
			}
			rsb.append(", sep = \"" + separator + "\")");
			// convert the stringbuiler to a string and execute
			// script will be of the form: FRAME$mynewcolumn <- paste(FRAME$Year, FRAME$Title, FRAME$Director, sep = ", ")
			String script = rsb.toString();
			frame.executeRScript(script);
			// update the metadata because column data has changed
			OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
			metaData.addProperty(table, table + "__" + newColName);
			metaData.setAliasToProperty(table + "__" + newColName, newColName);
			metaData.setDataTypeToProperty(table + "__" + newColName, "STRING");
			this.getFrame().syncHeaders();

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getNewColName() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// first input is what we want to name the new column
			String newColName = inputsGRS.getNoun(0).getValue() + "";
			if (newColName.length() == 0) {
				throw new IllegalArgumentException("Need to define the new column name");
			}
			return newColName;
		}
		throw new IllegalArgumentException("Need to define the new column name");
	}
	
	private String getSeparator() {
		NounMetadata input2 = this.getCurRow().getNoun(1);
		String separator = input2.getValue() + "";
		return separator;
	}
	
	private String getColumn(int i) {
		NounMetadata input = this.getCurRow().getNoun(i);
		String column = input.getValue() + "";
		return column;
	}
}
