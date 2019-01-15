package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class SortColumnReactor extends AbstractRFrameReactor {

	/**
	 * This reactor sorts a column based on a given sort direction The inputs to
	 * the reactor are:
	 * 1) the column to sort 
	 * 2) the sort direction
	 */
	
	public SortColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.SORT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();

		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get table name
		String table = frame.getName();

		// get inputs
		// the first input is the column to sort
		String column = this.keyValue.get(this.keysToGet[0]);
		if (column == null) {
			column = getSortColumn();
		}
		if (column.contains("__")) {
			column = column.split("__")[1];
		}

		// second input is the sort direction
		String sortDir = this.keyValue.get(this.keysToGet[0]);
		if (sortDir == null) {
			sortDir = getSortDirection();
		}

		// define the scripts based on the sort direction
		String script = null;
		if (sortDir == null || sortDir.equalsIgnoreCase("asc")) {
			script = table + " <- " + table + "[order(rank(" + column + "))]";
		} else if (sortDir.equalsIgnoreCase("desc")) {
			script = table + " <- " + table + "[order(-rank(" + column + "))]";
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"SortColumn", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		// execute the r script
		// script will be of the form: FRAME <- FRAME[order(rank(Director))]
		frame.executeRScript(script);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getSortColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// the first input will be the column to sort
			NounMetadata input1 = inputsGRS.getNoun(0);
			String fullColumn = input1.getValue() + "";
			if (fullColumn.length() == 0) {
				throw new IllegalArgumentException("Need to define the column to sort");
			}
			return fullColumn;
		}
		throw new IllegalArgumentException("Need to define the column to sort");
	}

	private String getSortDirection() {
		// the second input will be the sort direction
		NounMetadata input2 = this.getCurRow().getNoun(1);
		String sortDir = input2.getValue() + "";
		if (sortDir.length() == 0) {
			throw new IllegalArgumentException("Need to specify sort direction");
		}
		return sortDir;
	}
}
