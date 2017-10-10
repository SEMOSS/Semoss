package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class RenameColumnReactor extends AbstractRFrameReactor {
	
	@Override
	public NounMetadata execute() {
	
		// get frame
		RDataTable frame = null;
		if (this.insight.getDataMaker() != null) {
			frame = (RDataTable) getFrame();
		}

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		String originalColName = "";
		String updatedColName = ""; 
		
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//the first input will be the original column name
			NounMetadata input1 = inputsGRS.getNoun(0);
			PixelDataType nounType1 = input1.getNounType();
			if (nounType1 == PixelDataType.COLUMN) {
				String fullName = input1.getValue() + "";
				originalColName = fullName.split("__")[1];
			}
			//the second input will be the name that we want to use
		    NounMetadata input2 = inputsGRS.getNoun(1);
		    PixelDataType nounType2 = input2.getNounType();
		    if (nounType2 == PixelDataType.CONST_STRING) {
		    	updatedColName = input2.getValue() + "";
		    }
		    //check that the frame isn't null
		    if (frame != null) {
		    	String table = frame.getTableName();
		    	//ensure new header name is valid
		    	//make sure that the new name we want to use is valid
		    	String validNewHeader = getCleanNewHeader(table, updatedColName);
				//define the r script to be executed
		    	String script = "names(" + table + ")[names(" + table + ") == \"" + originalColName + "\"] = \"" + validNewHeader + "\"";
				//execute the r script
		    	//script is of the form: names(FRAME)[names(FRAME) == "Director"] = "directing_person"
		    	frame.executeRScript(script);
				// FE passes the column name
				// but meta will still be table __ column
		    	//update the metadata because column names have changed
				this.getFrame().getMetaData().modifyPropertyName(table + "__" + originalColName, table, table + "__" + validNewHeader);
				this.getFrame().syncHeaders();	
		    }
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	
}
