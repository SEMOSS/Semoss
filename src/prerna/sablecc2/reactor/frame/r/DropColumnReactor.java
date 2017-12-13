package prerna.sablecc2.reactor.frame.r;

import java.util.Arrays;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class DropColumnReactor extends AbstractRFrameReactor {

	/**
	 * This reactor drops columns from the frame. The inputs to the reactor are:
	 * 1) list of columns to drop
	 */
	
	public DropColumnReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// initialize rJavaTranslator
		init();
		
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		
		//get table name 
		String table = frame.getTableName();
		
		// get inputs
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// add loop; this would apply if more than one column to drop
			for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
				String column = getColumn(selectIndex);
				//clean the column name
				if (column.contains("__")) {
					column = column.split("__")[1];
				}
				
				// define the r script to be executed
				String script = table + "[," + column + ":=NULL]";
				
				//make sure that the column to be dropped exists; if not, throw error
				String[] allCol = getColumns(table);
				if (Arrays.asList(allCol).contains(column) != true) {
					throw new IllegalArgumentException("Column doesn't exist.");
				}

				// execute the script - it will be of the form:
				// FRAME[,FRAME__ColToDrop:=NULL]
				frame.executeRScript(script);

				// update the metadata because the columns are changing
				OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
				metaData.dropProperty(table + "__" + column, table);
				this.getFrame().syncHeaders();
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getColumn(int i) {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			String colName = inputsGRS.getNoun(i).getValue() + "";
			if (colName.length() == 0) {
				throw new IllegalArgumentException("Need to define the column to drop");
			}
			return colName;
		}
		throw new IllegalArgumentException("Need to define the column to drop");
	}
}
