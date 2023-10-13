package prerna.reactor.frame.r;

import java.util.ArrayList;
import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class RankReactor extends AbstractRFrameReactor {

	/**
	 * This reactor ranks the data based on a given column(s) and sort
	 * direction. The inputs to the reactor are: 1) the column(s) to be used for
	 * rank 2) the name of the rank column 3) the sorting order for each column
	 * 4) the partition column's to be used for rank
	 */

	private static final String PARTITION_BY_COLS = "partitionByCols";
	private static final String ASC = "ASC";
	private static final String DESC = "DESC";

	public RankReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey(),
				ReactorKeysEnum.SORT.getKey(), PARTITION_BY_COLS };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();

		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get frame name
		String frameName = frame.getName();

		// 1. get columns on which rank will be applied
		List<String> columns = getCols(ReactorKeysEnum.COLUMNS.getKey());
		// at least one column should be there
		if (columns.isEmpty()) {
			throw new IllegalArgumentException("Must pass at least one column for the rank");
		}

		// 2. get order of sorting to be applied to each column
		String order = getOrder(ReactorKeysEnum.SORT.getKey(), columns);

		// 3. get name of rank column
		String newColName = this.keyValue.get(ReactorKeysEnum.NEW_COLUMN.getKey());
		// checks
		if (newColName == null || newColName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the new column name");
		}
		// check if new colName is valid
		newColName = getCleanNewColName(newColName);

		StringBuilder script = new StringBuilder();
		// Using frank function for faster rank
		script.append(frameName).append("[,").append(newColName).append(":=frankv(.SD, ");
		String colVector = RSyntaxHelper.createStringRColVec(columns);
		script.append(colVector).append(",").append(order).append(", ties.method = \"dense\", na.last=TRUE)");

		// it will form the following script
		// ex.(by=list(\"Age_Range\",\"Relationship\"))
		List<String> partitionbyCols = getCols(PARTITION_BY_COLS);
		StringBuilder partitionByColString = new StringBuilder();
		if (!partitionbyCols.isEmpty()) {
			script.append(",by=list(");
			for (int partionColIndex = 0; partionColIndex < partitionbyCols.size(); partionColIndex++) {
				partitionByColString.append(partitionbyCols.get(partionColIndex));
				if (partionColIndex != partitionbyCols.size() - 1) {
					partitionByColString.append(",");
				}
			}
			script.append(partitionByColString);
			script.append(")");
		}
		script.append("]");
		// run rank script
		this.rJavaTranslator.runR(script.toString());
		this.addExecutedCode(script.toString());

		StringBuilder sortByRankScript = new StringBuilder();
		sortByRankScript.append(frameName).append(" <- ").append(frameName).append("[order(");
		// sorting partition by column ascending
		if (!partitionByColString.toString().isEmpty()) {
			sortByRankScript.append(partitionByColString + ",");
		}
		sortByRankScript.append(newColName).append(")]");
		// sort by rank
		this.rJavaTranslator.runR(sortByRankScript.toString());
		this.addExecutedCode(sortByRankScript.toString());

		// check if new column exists
		String colExistsScript = "\"" + newColName + "\" %in% colnames(" + frameName + ")";
		boolean colExists = this.rJavaTranslator.getBoolean(colExistsScript);
		if (!colExists) {
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to perform rank");
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// update meta data
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		metaData.addProperty(frameName, frameName + "__" + newColName);
		metaData.setAliasToProperty(frameName + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(frameName + "__" + newColName, SemossDataType.DOUBLE.toString());
		metaData.setDerivedToProperty(frameName + "__" + newColName, true);
		frame.syncHeaders();
		// to avoid the sorting of first column by default
		// this.insight.getPragmap().put("IMPLICIT_ORDER", false);

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(this.insight, frame, "Rank",
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully performed Rank."));
		return retNoun;
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private List<String> getCols(String key) {
		// first input is the columns on which rank will be applied
		// This method returns list of input column names
		List<String> columnsList = new ArrayList<>();
		GenRowStruct grs = this.store.getNoun(key);

		if (grs != null) {
			for (int i = 0; i < grs.size(); i++) {
				columnsList.add(grs.get(i).toString());
			}
		}
		return columnsList;
	}

	private String getOrder(String key, List<String> columns) {
		// third input is the sorting to be applied to each column

		StringBuilder order = new StringBuilder();
		GenRowStruct grs = this.store.getNoun(key);

		order.append("order = c(");

		for (int i = 0; i < columns.size(); i++) {
			// if no sort order is passed, ascending order will be applied
			if (grs == null || grs.isEmpty() || i >= grs.size()) {
				if (i != 0) {
					order.append(" , ");
				}
				order.append("1L");
			} else {
				// if sort order other than ASC or DESC, throw error
				if (!grs.get(i).toString().isEmpty() && grs.get(i).toString() != null
						&& !(grs.get(i).toString().equalsIgnoreCase(ASC)
								|| grs.get(i).toString().equalsIgnoreCase(DESC))) {
					throw new IllegalArgumentException("Column order not valid");
				} else {
					if (i != 0) {
						order.append(" , ");
					}
					// if sort = ASC or blank, then order will be ascending else
					// it will be descending
					if (grs.get(i).toString().equalsIgnoreCase(ASC)
							|| (grs.get(i).toString().isEmpty() || grs.get(i) == null)) {
						order.append("1L");
					} else {
						order.append("-1L");
					}
				}
			}

			if (i == columns.size() - 1) {
				order.append(")");
			}

		}

		return order.toString();
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(PARTITION_BY_COLS)) {
			return "The columns used for partitioning the rank";
		}
		return super.getDescriptionForKey(key);
	}

}
