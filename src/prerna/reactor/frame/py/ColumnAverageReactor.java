package prerna.reactor.frame.py;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class ColumnAverageReactor extends AbstractPyFrameReactor {

	/**
	 * This reactor averages columns 
	 * 1) the columns 
	 * 2) the new column name
	 */

	public ColumnAverageReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get the wrapper name
		// which is the framename with w in the end
		String wrapperFrameName = frame.getWrapperName();

		// get inputs
		List<String> columns = getColumns();
		String newColName = keyValue.get(this.keysToGet[1]);

		// checks
		if (newColName == null || newColName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the new column name");
		}

		// clean colName
		if (newColName.contains("__")) {
			String[] split = newColName.split("__");
			newColName = split[1];
		}
		// clean the column name to ensure that it is valid
		newColName = getCleanNewColName(frame, newColName);

		// build list of columns to use for average that can be executed in
		// Python
		StringBuilder colsAsPyList = new StringBuilder();
		colsAsPyList.append("[");
		for (String col : columns) {
			colsAsPyList.append("'" + col + "',");
		}
		colsAsPyList.append("]");

		// run script
		String script = wrapperFrameName + ".avg_cols(" + colsAsPyList.toString() + ", '" + newColName + "')";
		frame.runScript(script);
		this.addExecutedCode(script);

		// update meta data
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		String frameName = frame.getName();
		metaData.addProperty(frameName, frameName + "__" + newColName);
		metaData.setAliasToProperty(frameName + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(frameName + "__" + newColName, SemossDataType.DOUBLE.toString());
		metaData.setDerivedToProperty(frameName + "__" + newColName, true);
		frame.syncHeaders();

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, frame, "ColumnAverage",
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		// return the output
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully performed average across columns."));
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
			throw new IllegalArgumentException("Need to define the columns");
		}
		return columns;
	}
}
