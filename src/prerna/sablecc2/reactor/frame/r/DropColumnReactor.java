package prerna.sablecc2.reactor.frame.r;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.nounmeta.RemoveHeaderNounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

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
		
		// store the list of names being removed
		List<String> remCols = new Vector<String>();
		
		// get table name
		String table = frame.getTableName();

		// get inputs
		List<String> columnList = getColumns();
		String[] allCol = getColumns(table);

		// add loop; this would apply if more than one column to drop
		for (String column : columnList) {
			// clean the column name
			if (column.contains("__")) {
				column = column.split("__")[1];
			}
			remCols.add(column);

			// define the r script to be executed
			String script = table + " <- " + table + "[," + column + ":=NULL]";

			// make sure that the column to be dropped exists; if not, throw
			// error
			if (Arrays.asList(allCol).contains(column) != true) {
				throw new IllegalArgumentException("Column doesn't exist.");
			}

			// execute the script - it will be of the form:
			// FRAME[,FRAME__ColToDrop:=NULL]
			frame.executeRScript(script);

			// update the metadata because the columns are changing
			OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
			metaData.dropProperty(table + "__" + column, table);

			// drop filters with this column
			frame.getFrameFilters().removeColumnFilter(column);
		}
		// reset the frame headers
		frame.syncHeaders();

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"DropColumn", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new RemoveHeaderNounMetadata(remCols));
		return retNoun;
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private List<String> getColumns() {
		List<String> columns = new Vector<String>();

		GenRowStruct colGrs = this.store.getNoun(this.keysToGet[0]);
		if (colGrs != null && !colGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				columns.add(column);
			}
		} else {
			GenRowStruct inputsGRS = this.getCurRow();
			// keep track of selectors to change to upper case
			if (inputsGRS != null && !inputsGRS.isEmpty()) {
				for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
					String column = inputsGRS.get(selectIndex) + "";
					columns.add(column);
				}
			}
		}

		return columns;
	}
}
