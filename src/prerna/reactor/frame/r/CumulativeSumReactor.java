package prerna.reactor.frame.r;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class CumulativeSumReactor extends AbstractRFrameReactor {

	private static final String GROUP_BY_COLUMNS_KEY = "groupByCols";
	private static final String SORT_BY_COLUMNS_KEY = "sortCols";

	public CumulativeSumReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NEW_COLUMN.getKey(), ReactorKeysEnum.VALUE.getKey(),
				GROUP_BY_COLUMNS_KEY, SORT_BY_COLUMNS_KEY, ReactorKeysEnum.SORT.getKey() };
	}
	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String[] packages = new String[] { "data.table", "dplyr" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		String dataFrame = frame.getName();
		String newColName = this.keyValue.get(this.keysToGet[0]);
		if (newColName == null || newColName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the new column name");
		}
		// clean the column name to ensure that it is valid
		newColName = getCleanNewColName(frame, newColName);
		
		// check the column type to ensure the user uses numeric column for value
		String value = this.keyValue.get(this.keysToGet[1]);
		OwlTemporalEngineMeta metadata = this.getFrame().getMetaData();
		// determine if the value column datatype is int or double this will define the new column datatype
		SemossDataType dataType = metadata.getHeaderTypeAsEnum(dataFrame + "__" + value);

		if (value == null || value.isEmpty()) {
			throw new IllegalArgumentException("Need to define the value to aggregate sum");
		}
		if(!Utility.isNumericType(dataType.toString())) {
			throw new IllegalArgumentException("Need to aggregate on numerical column type");
		}
		
		// optional value to group by
		List<String> groupCols =  getGroupByColumns();
		if(groupCols == null) {
			
		}
		// optional value to sort by
		List<String> sortColumns = getSortByColumns();

		// write the script to run csum
		StringBuilder rsb = new StringBuilder();
		String outputFrame = Utility.getRandomString(5);
		// sort the frame
		rsb.append(outputFrame).append(" = ").append(dataFrame);
		// add group by
		String groupBy = " %>% group_by( ";
		String implicitSort = "";
		for(int i = 0; i < groupCols.size(); i++){
			String col = groupCols.get(i);
			groupBy += col;
			implicitSort += col;
			if(i < groupCols.size() -1) {
				groupBy += ",";
				implicitSort += ",";
			}
		}
		groupBy += ") ";
		implicitSort += ",";

		if (!groupCols.isEmpty()) {
			rsb.append(groupBy);
		}
		
		// TODO find a way to sum based on direction asc is the default for now
		String sortDir = this.keyValue.get(this.keysToGet[4]);
		if (sortDir == null || sortDir.equalsIgnoreCase("asc")) {
			
		} else if (sortDir.equalsIgnoreCase("desc")) {
			
		}
		
		// add sort by
		String sortBy = " %>% arrange( ";
		if (!groupCols.isEmpty()) {
			sortBy += implicitSort;
		}
		for(int i = 0; i < sortColumns.size(); i++){
			String col = sortColumns.get(i);
			sortBy += col;
			if(i < sortColumns.size() -1) {
				sortBy += ",";
			}
		}
		sortBy += ") ";
		
		if (!sortColumns.isEmpty()) {
			rsb.append(sortBy);
		}		
		
		// add cumsum function
		rsb.append("%>% mutate(" + newColName + "=cumsum(" + value + "))");

		// run csum
		// excute csum script
		frame.executeRScript(rsb.toString());
		this.addExecutedCode(rsb.toString());

		// check if the routine is successful
		// if it is successful add the new column
		// update the metadata to include this new column
		boolean success = this.rJavaTranslator.varExists(outputFrame);
		if(!success) {
			throw new IllegalArgumentException("Unable to generate Cumulative Sum");
		}
		frame.executeRScript(RSyntaxHelper.asDataTable(dataFrame, outputFrame) + "; rm("+outputFrame+")");
		metadata.addProperty(dataFrame, dataFrame + "__" + newColName);
		metadata.setAliasToProperty(dataFrame + "__" + newColName, newColName);
		metadata.setDataTypeToProperty(dataFrame + "__" + newColName, dataType.toString());

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColName));
		return retNoun;
	}

	private List<String> getGroupByColumns() {
		List<String> colInputs = new Vector<String>();
		GenRowStruct colGRS = this.store.getNoun(GROUP_BY_COLUMNS_KEY);
		if (colGRS != null) {
			int size = colGRS.size();
			if (size > 0) {
				for (int i = 0; i < size; i++) {
					// get each individual column entry and clean
					String column = colGRS.get(i).toString();
					if (column.contains("__")) {
						column = column.split("__")[1];
					}
					colInputs.add(column);
				}
			}
		}
		return colInputs;
	}

	private List<String> getSortByColumns() {
		List<String> colInputs = new Vector<String>();
		GenRowStruct colGRS = this.store.getNoun(SORT_BY_COLUMNS_KEY);
		if (colGRS != null) {
			int size = colGRS.size();
			if (size > 0) {
				for (int i = 0; i < size; i++) {
					// get each individual column entry and clean
					String column = colGRS.get(i).toString();
					if (column.contains("__")) {
						column = column.split("__")[1];
					}
					colInputs.add(column);
				}
			}
		}
		return colInputs;
	}

}
