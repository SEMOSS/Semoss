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
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class CumulativeSumReactor extends AbstractPyFrameReactor{

	/**
	 * This reactor performs the cumulative sum by grouping data in a columns
	 * Input Keys are as follows:
	 * 0)	newCol =	Name of the new column being created
	 * 1)	value =	The instance value in a column, or the numeric or string value used in a operation
	 * 2)	groupByCols = List of columns used to groupBy cumulative sum
	 * 3)	sortCols = List of Columns used to sort data
	 * 4)	sort =	Sort direction: ascending ("asc") or descending ("desc")
	 */
	

	private static final String GROUP_BY_COLUMNS_KEY = "groupByCols";
	private static final String SORT_BY_COLUMNS_KEY = "sortCols";

	public CumulativeSumReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NEW_COLUMN.getKey(), ReactorKeysEnum.VALUE.getKey(),
				GROUP_BY_COLUMNS_KEY, SORT_BY_COLUMNS_KEY, ReactorKeysEnum.SORT.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		PandasFrame frame = (PandasFrame) getFrame();
		// get the frame name
		String frameName = frame.getName();
		// get inputs
		String newColName = this.keyValue.get(this.keysToGet[0]);
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
		// TODO check the column types ensure the user uses numeric column for value
		String value = this.keyValue.get(this.keysToGet[1]);
		// TODO determine if the value column datatype is int or double this will define the new column datatype
		if (value == null || value.isEmpty()) { //check
			throw new IllegalArgumentException("Need to define the value to aggregate sum");
		}
		
		// optional value to group by
		List<String> groupCols =  getGroupByColumns();
		StringBuilder colsAsPyList = new StringBuilder();
		if (!groupCols.isEmpty()) {
			// otherwise build list of columns to use for groupBy that can be
			// executed in Python
			colsAsPyList.append("[");
			for (String col : groupCols) {
				colsAsPyList.append("'" + col + "',");
			}
			colsAsPyList.append("]");
		}
		
		// optional value to sort by
		List<String> sortColumns = getSortByColumns();
		StringBuilder sortColsAsPyList = new StringBuilder();
		if (!sortColumns.isEmpty()) {
			// otherwise build list of columns to use for sortBy that can be
			// executed in Python
			sortColsAsPyList.append("[");
			for (String col : sortColumns) {
				sortColsAsPyList.append("'" + col + "',");
			}
			sortColsAsPyList.append("]");
		}

		// define the script to be executed;
		// this assigns a new column name with no data in columns
		String newColumnSelector = frameName + "['" + newColName + "']";
				
		// run script
		if (!sortColumns.isEmpty()) {
			String script = frameName + ".sort_values(by=" + sortColsAsPyList.toString()
					+ ", ascending=False, na_position='last', inplace=True, ignore_index=True)";
			frame.runScript(script);
			this.addExecutedCode(script);
		}
		String groupBySyntax = "";
		// TODO make groupCOl optional
		if(!groupCols.isEmpty()) {
			groupBySyntax = ".groupby(" + colsAsPyList.toString() + ")";
		}
		String script = newColumnSelector + "= " + frameName + groupBySyntax + "['" + value + "'].cumsum()";
		frame.runScript(script);
		this.addExecutedCode(script);
		
		// check if operation was successful
		boolean success = this.insight.getPyTranslator().getBoolean("'" + newColName + "' in " + frameName);
		if(!success) {
			throw new IllegalArgumentException("Unable to generate Cumulative Sum");
		}
		// update the metadata to include this new column
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		metaData.addProperty(frameName, frameName + "__" + newColName);
		metaData.setAliasToProperty(frameName + "__" + newColName, newColName);

		metaData.setDataTypeToProperty(frameName + "__" + newColName, SemossDataType.DOUBLE.toString());
		// TODO do we need this?
		script = newColumnSelector + "= pd.to_numeric(" + newColumnSelector + ", errors='coerce')";
		frame.runScript(script);
		this.addExecutedCode(script);

		frame.syncHeaders();		
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, frame, "ColumnAggSum",
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
				
		// return the output
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColName)); //need this to show newly added column
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully performed Cumulative Sum."));
		return retNoun;
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	//to group by this list of columns
	private List<String> getGroupByColumns() {
		List<String> columns = new Vector<String>();
		GenRowStruct colGrs = this.store.getNoun(GROUP_BY_COLUMNS_KEY);
		//GenRowStruct colGrs = this.store.getNoun(this.keysToGet[0]);
		if (colGrs != null && !colGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				columns.add(column);
			}
		} else {
			throw new IllegalArgumentException("Need to define the group by columns");
		}
		return columns;
	}

	//to sort by this list of columns
	private List<String> getSortByColumns() {
		List<String> columns = new Vector<String>();
		GenRowStruct colGrs = this.store.getNoun(SORT_BY_COLUMNS_KEY);
		//GenRowStruct colGrs = this.store.getNoun(this.keysToGet[0]);
		if (colGrs != null && !colGrs.isEmpty()) {
			for (int selectIndex = 0; selectIndex < colGrs.size(); selectIndex++) {
				String column = colGrs.get(selectIndex) + "";
				columns.add(column);
			}
		} 
		return columns;
	}
}
