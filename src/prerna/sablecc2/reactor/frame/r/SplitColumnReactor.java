package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Utility;

public class SplitColumnReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {
		//use init to initialize rJavaTranslator object that will be used later
		init();
		// get frame
		RDataTable frame = null;
		if (this.insight.getDataMaker() != null) {
			frame = (RDataTable) getFrame();
		}

		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		//get table name
		String table = frame.getTableName();
		//make a temporary table name
		//we will reassign the table to this variable
		//then assign back to the original table name
		String tempName = Utility.getRandomString(8);
		//script to change the name of the table back to the original name - will be used later 
		String frameReplaceScript = table + " <- " + tempName + ";"; 
		//false columnReplaceScript indicates that we will not drop the original column of data
		String columnReplaceScript = "FALSE";
		String direction = "wide";

		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			//value we are splitting the column at is the first noun inputted
			NounMetadata separatorNoun = inputsGRS.getNoun(0); 
			String separator = separatorNoun.getValue() + "";

			for (int i = 1; i < inputsGRS.size(); i++) {
				//next input will be the column that we are splitting
				//we can specify to split more than one column, so there could be multiple column inputs
				NounMetadata input = inputsGRS.getNoun(i);
				PixelDataType nounType = input.getNounType();
				if (nounType == PixelDataType.COLUMN) {
					String fullColumn = input.getValue() + "";
					String column = "";
					if (fullColumn.contains("__")) {
						column = fullColumn.split("__")[1];
					}

					//build the script to execute
					String script = tempName + " <- cSplit(" + table + ", "
							+ "\"" + column
							+ "\", \"" + separator
							+ "\", direction = \"" + direction
							+ "\", drop = " + columnReplaceScript+ ");" 
							;

					//evaluate the r script
					frame.executeRScript(script);

					//get all the columns that are factors
					script = "sapply(" + tempName + ", is.factor);";
					//keep track of which columns are factors
					int [] factors = this.rJavaTranslator.getIntArray(script);			
					String [] colNames = getColumns(tempName);

					// now I need to compose a string based on it
					//we will convert the columns that are factors into strings using as.character
					String conversionString = "";
					for(int factorIndex = 0;factorIndex < factors.length;factorIndex++)
					{
						if(factors[factorIndex] == 1) // this is a factor
						{
							conversionString = conversionString + 
									tempName + "$" + colNames[factorIndex] + " <- "
									+ "as.character(" + tempName + "$" + colNames[factorIndex] + ");";
						}
					}

					//convert factors
					frame.executeRScript(conversionString);
					//change table back to original name
					frame.executeRScript(frameReplaceScript);
					// perform variable cleanup
					frame.executeRScript("rm(" + tempName + "); gc();");
					//column header data is changing so we must recreate metadata
					recreateMetadata(table);
				}
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
