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
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class ExtractLettersReactor extends AbstractFrameReactor {
	public static final String ALPHA_COLUMN_NAME = "_ALPHA";

	public ExtractLettersReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		// get table name
		String table = frame.getName() ;
		// get columns to extract alphabet characters
		List<String> columns = getColumns();
		// check if user want to override the column or create new columns
		boolean overrideColumn = getOverride();
		// we need to check data types this will only be valid on non numeric values
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		// update existing columns
		if (overrideColumn) {
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				// check data type this is only valid on non numeric values
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(table + "__" + column);
				if (Utility.isStringType(dataType.toString())) {
					try {
						frame.runScript(table + ".extract_alpha('" + column + "')");
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					throw new IllegalArgumentException("Column type must be string");
				}
			}
		}
		
		// create new column
		else {
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(table + "__" + column);
				if (Utility.isStringType(dataType.toString())) {
					String newColumn = getCleanNewColName(table, column + ALPHA_COLUMN_NAME);
					frame.runScript(table + ".extract_alpha('" + column + "',  '" + newColumn + "')");
					
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
