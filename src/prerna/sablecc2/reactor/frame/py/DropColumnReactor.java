package prerna.sablecc2.reactor.frame.py;

import java.util.List;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.nounmeta.RemoveHeaderNounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class DropColumnReactor extends AbstractFrameReactor {

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
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();

		// get table name
		String table = frame.getName();

		// store the list of names being removed
		List<String> remCols = new Vector<String>();
		
		// get inputs
		List<String> columns = getColumns();
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < columns.size(); i++) {
			String col = columns.get(i);
			if (col.contains("__")) {
				String[] split = col.split("__");
				col = split[1];
				table = split[0];
			}
			// define the script to be executed
			
			builder.append(table + ".drop_col('" + col + "')");
			remCols.add(col);

			metaData.dropProperty(frame.getName() + "__" + col, frame.getName());
			// drop filters with this column
			frame.getFrameFilters().removeColumnFilter(col);
		}

		// run the script
		
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
