package prerna.reactor.frame.r;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class ExtractLettersReactor extends AbstractRFrameReactor {
	public static final String ALPHA_COLUMN_NAME = "_ALPHA";

	public ExtractLettersReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		// get table name
		String table = frame.getName();
		// get columns to extract alphabet characters
		List<String> columns = getColumns();
		// check if user want to override the column or create new columns
		boolean overrideColumn = getOverride();
		// we need to check data types this will only be valid on non numeric values
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		
		List<PixelOperationType> opTypes = new Vector<PixelOperationType>();
		opTypes.add(PixelOperationType.FRAME_DATA_CHANGE);
		// update existing columns
		if (overrideColumn) {
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				// check data type this is only valid on non numeric values
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(table + "__" + column);
				if(dataType == null)
					return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");

				if (Utility.isStringType(dataType.toString())) {
					String update = table + "$" + column + " <- gsub('[^a-zA-Z_]', '', " + table + "$" + column + ")";
					frame.executeRScript(update);
					this.addExecutedCode(update);
				} else {
					throw new IllegalArgumentException("Column type must be string");
				}
			}
		}
		// create new column
		else {
			opTypes.add(PixelOperationType.FRAME_HEADERS_CHANGE);
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(table + "__" + column);
				if (Utility.isStringType(dataType.toString())) {
					String newColumn = getCleanNewColName(frame, column + ALPHA_COLUMN_NAME);
					String update = table + "$" + newColumn + " <- \"\";";
					update += table + "$" + newColumn + " <- gsub('[^a-zA-Z_]', '', " + table + "$" + column + ");";
					this.rJavaTranslator.runR(update);
					this.addExecutedCode(update);
					metaData.addProperty(table, table + "__" + newColumn);
					metaData.setAliasToProperty(table + "__" + newColumn, newColumn);
					metaData.setDataTypeToProperty(table + "__" + newColumn, SemossDataType.STRING.toString());
				} else {
					throw new IllegalArgumentException("Column type must be string");
				}
			}
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"ExtractAlphaChars", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, opTypes);
	}

	private List<String> getColumns() {
		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
		Vector<String> columns = new Vector<String>();
		NounMetadata noun;
		if (grs != null) {
			for (int i = 0; i < grs.size(); i++) {
				noun = grs.getNoun(i);
				if (noun != null) {
					String column = noun.getValue() + "";
					if (column.length() > 0) {
						columns.add(column);
					}
				}
			}
		}
		return columns;
	}

	private boolean getOverride() {
		GenRowStruct grs = this.store.getNoun(keysToGet[1]);
		boolean override = false;
		NounMetadata noun;
		if (grs != null) {
			noun = grs.getNoun(0);
			override = (Boolean) noun.getValue();
		}
		return override;
	}

}
