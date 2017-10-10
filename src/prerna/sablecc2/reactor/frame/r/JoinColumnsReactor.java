package prerna.sablecc2.reactor.frame.r;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class JoinColumnsReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {
		// get frame
		RDataTable frame = null;
		if (this.insight.getDataMaker() != null) {
			frame = (RDataTable) getFrame();
		}

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		String table = frame.getTableName(); 
		StringBuilder rsb = new StringBuilder();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//first input is what we want to name the new column
			NounMetadata input1 = inputsGRS.getNoun(0);
			String newColName = input1.getValue() + "";
			//second input is the delimeter
			NounMetadata input2 = inputsGRS.getNoun(1);
			String separator = input2.getValue() + "";
			rsb.append(table + "$" + newColName + " <- paste(");
			//the remaining inputs are all of the columns that we want to join
			for (int i = 2; i < inputsGRS.size(); i++) {
				NounMetadata input = inputsGRS.getNoun(i);
				String fullColumn = input.getValue() + ""; 
				String column = fullColumn;
				//separate the column name from the frame name
				if (fullColumn.contains("__")) {
					column = fullColumn.split("__")[1]; 
				}
				//begin building the stringbuilder for the r script
				rsb.append(table + "$"  + column); 
				if (i < inputsGRS.size() - 1) {
					//add a comma between each column entry
					rsb.append(", ");  
				}
			}
			rsb.append(", sep = \"" + separator + "\")");
			//convert the stringbuiler to a string and execute
			//script will be of the form: FRAME$mynewcolumn <- paste(FRAME$Year, FRAME$Title, FRAME$Director, sep = ", ")
			String script = rsb.toString();
			frame.executeRScript(script);
			//update the metadata because column data has changed
			OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
			metaData.addProperty(table, table + "__" + newColName);
			metaData.setAliasToProperty(table + "__" + newColName, newColName);
			metaData.setDataTypeToProperty(table + "__" + newColName, "STRING");
			this.getFrame().syncHeaders();		
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
