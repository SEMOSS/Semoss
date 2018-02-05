package prerna.sablecc2.reactor.frame.r;

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

public class ExtractNumbersReactor extends AbstractRFrameReactor {
	public static final String OVERRIDE = "override";
	public static final String NUMERIC_COLUMN_NAME = "_NUMERIC";
	
	public ExtractNumbersReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey(), OVERRIDE};
	}

	@Override
	public NounMetadata execute() {
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		// get table name
		String table = frame.getTableName();
		// get columns to extract numeric characters
		List<String> columns = getColumns();
		// check if user want to override the column or create new columns
		boolean overrideColumn = getOverride();
		// we need to check data types this will only be valid on non numeric values
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		// update existing columns
		if (overrideColumn) {
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(table + "__" + column);
				if (dataType != SemossDataType.INT && dataType != SemossDataType.DOUBLE) {
					String script = table + "$" + column + " <- as.numeric(gsub('[^-\\\\.0-9]', '', " + table + "$" + column + "));";
					try {
						frame.executeRScript(script);
						frame.getMetaData().modifyDataTypeToProperty(table + "__" + column, table, SemossDataType.DOUBLE.toString());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		// create new column
		else {
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(table + "__" + column);
				if (dataType != SemossDataType.INT && dataType != SemossDataType.DOUBLE) {
					String newColumn = getCleanNewColName(table, column + NUMERIC_COLUMN_NAME);
					String update = table + "$" + newColumn + " <- \"\";";
					frame.executeRScript(update);
					update = table + "$" + newColumn + " <- as.numeric(gsub('[^-\\\\.0-9]', '', " + table + "$" + column + "));";
					frame.executeRScript(update);
					metaData.addProperty(table, table + "__" + newColumn);
					metaData.setAliasToProperty(table + "__" + newColumn, newColumn);
					metaData.setDataTypeToProperty(table + "__" + newColumn, SemossDataType.DOUBLE.toString());
				}
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
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
		GenRowStruct grs = this.store.getNoun(OVERRIDE);
		boolean override = false;
		NounMetadata noun;
		if (grs != null) {
			noun = grs.getNoun(0);
			override = (Boolean) noun.getValue();
		}
		return override;
	}

	///////////////////////// KEYS /////////////////////////////////////
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(OVERRIDE)) {
			return "Indicates if the existing column will be overridden or if a new column will be created with \"_NUMERIC\" appended";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
