package prerna.sablecc2.reactor.frame.py;

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

public class RankReactor extends AbstractPyFrameReactor {

	/**
	 * This reactor ranks the data based on a given column(s) and sort direction. The inputs to
	 * the reactor are:
	 * 1) the column(s) to be used for rank 
	 * 2) the name of the rank column
	 * 3) the sorting order for each column
	 */

	private static final String PARTITION_BY_COL = "partitionByCol";
	private static final String ASC = "ASC";
	private static final String DESC = "DESC";
	
	public RankReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey(), 
				ReactorKeysEnum.SORT.getKey(), PARTITION_BY_COL};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get the wrapper name
		// which is the frame name with w in the end
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
		//partition by ex.(by=\"Age_Range\")
		String partitionbyCol = this.keyValue.get(PARTITION_BY_COL);
		
		StringBuilder finalRankScript = new StringBuilder();
		StringBuilder sortByRankScript = new StringBuilder();
		StringBuilder colsArrayScript = new StringBuilder();
		StringBuilder dropTempRankColsScript = new StringBuilder();	
		
		if (partitionbyCol != null && !partitionbyCol.equals("")) {

			StringBuilder sortValues = new StringBuilder();
			StringBuilder tempRankScript = new StringBuilder();
			colsArrayScript.append("['").append(partitionbyCol).append("'");

			sortValues.append("[True ");

			for (int i = 0; i < columns.size(); i++) {
				colsArrayScript.append(", '" + columns.get(i) + "'");
				sortValues.append(", " + getSortOrder(i, ReactorKeysEnum.SORT.getKey()));

			}
			colsArrayScript.append("]");
			sortValues.append("]");

			frame.runScript("cols= " + colsArrayScript.toString());
			// sort and groupby + ngroup to label each group with your ranking
			tempRankScript.append(frame.getName()).append("['TempRank'] =").append(frame.getName())
					.append(".sort_values(cols,").append(" ascending=").append(sortValues).append(")")
					.append(".groupby(").append("cols,").append("sort=False, dropna=True).ngroup()");

			frame.runScript(tempRankScript.toString());
			// Subtracting the minimum rank within each 'key' then gives the
			// desired ranking within group
			finalRankScript.append(frame.getName()).append("['").append(newColName).append("']").append("=")
					.append(frame.getName()).append("['TempRank'] - ").append(frame.getName()).append(".groupby('")
					.append(partitionbyCol).append("')['TempRank'].transform('min') + 1");

			sortByRankScript.append(frame.getName()).append(".sort_values(['").append(partitionbyCol).append("','")
					.append(newColName).append("'], inplace=True)");

			dropTempRankColsScript.append(frame.getName()).append(" = ").append(frame.getName())
					.append(".drop(columns=['TempRank'])");

		} else {

			StringBuilder createColsArray = new StringBuilder();
			// createColsArray.append("cols = [");
			for (int i = 0; i < columns.size(); i++) {
				StringBuilder rankScript = new StringBuilder();
				rankScript.append(frame.getName()).append("[\"").append(columns.get(i)).append("Rank").append("\"] = ")
						.append(frame.getName()).append("[\"").append(columns.get(i))
						.append("\"].rank(method = 'min',na_option='bottom',ascending=")
						.append(getSortOrder(i, ReactorKeysEnum.SORT.getKey())).append(")");

				// running script to rank each column individually
				frame.runScript(rankScript.toString());

				createColsArray.append("'").append(columns.get(i)).append("Rank").append("'");
				if (i != columns.size() - 1) {
					createColsArray.append(",");
				}
			}

			finalRankScript.append(frame.getName()).append("['").append(newColName).append("'] =")
					.append(frame.getName())
					.append(".sort_values(cols, ascending=True).groupby(cols, sort=False,dropna=True).ngroup() + 1");

			sortByRankScript.append(frame.getName()).append(".sort_values(['").append(newColName)
					.append("'], inplace=True)");

			dropTempRankColsScript.append(frame.getName()).append(" = ").append(frame.getName())
					.append(".drop(columns=[ ").append(createColsArray).append("])");

			// create array of columns which is passed to groupby
			colsArrayScript.append("cols = [").append(createColsArray).append("]");
			frame.runScript(colsArrayScript.toString());

		}
		
		// running script to generate final rank
		frame.runScript(finalRankScript.toString());

		// running script to sort by final rank column
		frame.runScript(sortByRankScript.toString());

		// run script to drop intermediate rank columns as we only need the
		// final rank
		frame.runScript(dropTempRankColsScript.toString());
		
		//update wrapperFrameName it will end up frame name with 'w'
		frame.runScript(wrapperFrameName+".cache['data'][['"+newColName+"']]"+"="+frame.getName()+"['"+newColName+"']");;

		// update meta data
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		String frameName = frame.getName();
		metaData.addProperty(frameName, frameName + "__" + newColName);
		metaData.setAliasToProperty(frameName + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(frameName + "__" + newColName, SemossDataType.DOUBLE.toString());
		metaData.setDerivedToProperty(frameName + "__" + newColName, true);
		frame.syncHeaders();
        //to avoid the sorting of first column by default
//		this.insight.getPragmap().put("IMPLICIT_ORDER", false);
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, frame, "Rank",
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		// return the output
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);

		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully performed Rank"));
		return retNoun;
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
		
	private List<String> getColumns() {
		//first input is the columns on which rank will be applied
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
	
	
	//get the sort order for each column
	private String getSortOrder(int index,String key) {
		//third input is the sorting to be applied to each column
		GenRowStruct grs = this.store.getNoun(key);

		//if no sort order is passed, ascending order will be applied
		if(grs == null || grs.isEmpty() || index>=grs.size()) {				
			return "True";			
		} else {
			//if sort order other than ASC or DESC, throw error
			if(!grs.get(index).toString().isEmpty() && grs.get(index).toString() != null
					&& !(grs.get(index).toString().equalsIgnoreCase(ASC)
							|| grs.get(index).toString().equalsIgnoreCase(DESC))) {
				throw new IllegalArgumentException("Column order not valid");
			} else {
				//if sort = ASC or blank, then order will be ascending else it will be descending
				if(grs.get(index).toString().equalsIgnoreCase(ASC) || grs.get(index).toString().isEmpty()) {
					return "True";
				} else {
					return "False";
				}
			}
		}
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(PARTITION_BY_COL)) {
			return "This column used for partitioning the Rank";
		}
		return super.getDescriptionForKey(key);
	}
	
}
