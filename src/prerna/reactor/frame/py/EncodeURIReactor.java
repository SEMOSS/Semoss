package prerna.reactor.frame.py;

import java.util.List;
import java.util.Vector;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class EncodeURIReactor extends AbstractPyFrameReactor{
	
	/**
	 * This reactor encodes special characters in columns to conform to URI standards
	 */
	
	public EncodeURIReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		
		// get frame name
		String table = frame.getName();
		
		// get wrapper name
		String wrapperFrameName = frame.getWrapperName();

		// get inputs
		List<String> columnNames = getColumns();
		
		// iterate through all passed columns
		for (String col : columnNames) {
			if (col.contains("__")) {
				String[] split = col.split("__");
				col = split[1];
				table = split[0];
			}
			String dataType = metaData.getHeaderTypeAsString(table + "__" + col);
			if (dataType.equalsIgnoreCase("STRING")) {
				// build script
				String script = wrapperFrameName + ".encode_uri('" + col + "')";
				// run script
				frame.runScript(script);
				this.addExecutedCode(script);
			}
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"EncodeURI", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
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
