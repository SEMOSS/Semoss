package prerna.reactor.frame.py;

import java.util.Arrays;
import java.util.List;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.poi.main.HeadersException;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class UnpivotReactor extends AbstractPyFrameReactor {

	public UnpivotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		String wrapperName = frame.getWrapperName();
		// get column inputs in an array
		String[] columns = getColumns();
		String frameName = frame.getName();

		// makes the columns and converts them into
		// list ['col1', 'col2']
		StringBuilder valueColumns = new StringBuilder();
		valueColumns.append("[");
		for (int i = 0; i < columns.length; i++) {
			valueColumns.append("'" + columns[i] + "'");
			if (i + 1 < columns.length)
				valueColumns.append(", ");
		}
		valueColumns.append("]");

		String script = frameName + " = " + wrapperName + ".unpivot(" + valueColumns + ")";
		frame.runScript(script);
		this.addExecutedCode(script);

		HeadersException headerChecker = HeadersException.getInstance();
		List<String> allColumns = Arrays.asList(getColumns(frame));
		// python unpivot creates two columns variable and value
		// we make the assumption that the start headers
		// are already clean and not duplicating
		String variableName = "variable";
		String valueName = "value";
		String[] newColumns = headerChecker.cleanAndMatchColumnNumbers(variableName, valueName, allColumns);
		String newVarName = newColumns[0];
		String newValueName = newColumns[1];

		// rename variable name
		String rename = PandasSyntaxHelper.alterColumnName(frameName, variableName, newVarName);
		frame.runScript(rename);
		this.addExecutedCode(rename);
		// rename value name
		rename = PandasSyntaxHelper.alterColumnName(frameName, valueName, newValueName);
		frame.runScript(rename);
		this.addExecutedCode(rename);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, frame, "Unpivot",
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		frame = (PandasFrame) recreateMetadata(frame);

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String[] getColumns() {
		// get columns from key
		String[] columns = null;
		GenRowStruct colGrs = this.store.getNoun(this.keysToGet[0]);
		if (colGrs != null && !colGrs.isEmpty()) {
			columns = new String[colGrs.size()];
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				columns[selectIndex] = column;
			}
			return columns;
		} else {
			// get columns from index
			GenRowStruct inputsGRS = this.getCurRow();
			columns = new String[inputsGRS.size()];
			if (inputsGRS != null && !inputsGRS.isEmpty()) {
				// input is the columns to unpivot
				for (int i = 0; i < inputsGRS.size(); i++) {
					NounMetadata input = this.getCurRow().getNoun(i);
					String column = input.getValue() + "";
					// clean column
					if (column.contains("__")) {
						column = column.split("__")[1];
						// add the columns to keep to the string array
					}
					columns[i] = column;
				}
				return columns;
			}
		}
		throw new IllegalArgumentException("Need to define columns to unpivot");
	}
}