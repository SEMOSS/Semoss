package prerna.reactor.frame.r;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Separates a column at the specified index creating two new columns on the
 * left and right of the index
 */
public class SeparateColumnReactor extends AbstractRFrameReactor {
	
	/**
	 * R example 
	 * library(tidyr) 
	 * df <- data.frame(date = c(201401, 201402, 201403,201412), test=c('a', 'b', 'c', 'd')) 
	 * df = df %>% separate(date, into = c('year', 'month'), sep = 4, remove=FALSE)
	 */
	
	private static final String LEFT_COLUMN_NAME_INPUT = "lName";
	private static final String RIGHT_COLUMN_NAME_INPUT = "rName";

	public SeparateColumnReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.INDEX.getKey(), LEFT_COLUMN_NAME_INPUT, RIGHT_COLUMN_NAME_INPUT};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();
		String[] packages = new String[] { "tidyr" };
		this.rJavaTranslator.checkPackages(packages);
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta metadata = this.getFrame().getMetaData();
		String dataFrame = frame.getName();
		// get inputs
		String column = this.keyValue.get(this.keysToGet[0]);
		if (column == null || column.isEmpty()) {
			throw new IllegalArgumentException("Need to define the column to separate");
		}
		String index = this.keyValue.get(this.keysToGet[1]);
		if (index == null || index.isEmpty()) {
			throw new IllegalArgumentException("Need to define the index to separate column");
		}
		// clean new column names
		String leftColumnName = this.keyValue.get(LEFT_COLUMN_NAME_INPUT);
		if (leftColumnName == null || leftColumnName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the left column name");
		}
		leftColumnName = getCleanNewColName(frame, leftColumnName);
		String rightColumnName = this.keyValue.get(RIGHT_COLUMN_NAME_INPUT);
		if (rightColumnName == null || rightColumnName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the right column name");
		}
		rightColumnName = getCleanNewColName(frame, rightColumnName);

		StringBuilder sb = new StringBuilder();

		sb.append(dataFrame).append(" = ").append(dataFrame).append(" %>% separate(").append(column)
				.append(", into = c('").append(leftColumnName).append("', '").append(rightColumnName)
				.append("'), sep = ").append(index).append(", remove=FALSE)");

		frame.executeRScript(sb.toString());
		this.addExecutedCode(sb.toString());

		// check if new column exists
		String colExistsScript = "\"" + leftColumnName + "\" %in% colnames(" + dataFrame + ")";
		boolean colExists = this.rJavaTranslator.getBoolean(colExistsScript);
		if (!colExists) {
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to separate column");
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		
		// update meta data
		metadata.addProperty(dataFrame, dataFrame + "__" + leftColumnName);
		metadata.setAliasToProperty(dataFrame + "__" + leftColumnName, leftColumnName);
		metadata.setDataTypeToProperty(dataFrame + "__" + leftColumnName, SemossDataType.STRING.toString());
		metadata.setDerivedToProperty(dataFrame + "__" + leftColumnName, true);

		metadata.addProperty(dataFrame, dataFrame + "__" + rightColumnName);
		metadata.setAliasToProperty(dataFrame + "__" + rightColumnName, rightColumnName);
		metadata.setDataTypeToProperty(dataFrame + "__" + rightColumnName, SemossDataType.STRING.toString());
		metadata.setDerivedToProperty(dataFrame + "__" + rightColumnName, true);
		frame.syncHeaders();


		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully separated " + column + " ."));
		return retNoun;
	}
	
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(LEFT_COLUMN_NAME_INPUT)) {
			return "The new column name for the left side";
		} else if(key.equals(RIGHT_COLUMN_NAME_INPUT)) {
			return "The new column name for the right side";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
