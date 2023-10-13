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
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class ExtractNumbersReactor extends AbstractPyFrameReactor {
	
	public static final String NUMERIC_COLUMN_NAME = "_NUMERIC";
	
	public ExtractNumbersReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		// get table name
		String wrapperFrameName = frame.getWrapperName();
		// get columns to extract numeric characters
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
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(frame.getName() + "__" + column);
				if (Utility.isStringType(dataType.toString())) {
					try {
						String script = wrapperFrameName + ".extract_num('" + column + "')";
						frame.runScript(script);
						this.addExecutedCode(script);
						frame.getMetaData().modifyDataTypeToProperty(frame.getName() + "__" + column, 
								frame.getName(), SemossDataType.DOUBLE.toString());
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
			opTypes.add(PixelOperationType.FRAME_HEADERS_CHANGE);
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(frame.getName() + "__" + column);
				if (Utility.isStringType(dataType.toString())) {
					String newColumn = getCleanNewColName(frame, column + NUMERIC_COLUMN_NAME);
					String script = wrapperFrameName + ".extract_num('" + column + "',  '" + newColumn + "')";
					frame.runScript(script);
					this.addExecutedCode(script);
					metaData.addProperty(frame.getName(), frame.getName() + "__" + newColumn);
					metaData.setAliasToProperty(frame.getName() + "__" + newColumn, newColumn);
					metaData.setDataTypeToProperty(frame.getName() + "__" + newColumn, SemossDataType.DOUBLE.toString());
				} else {
					throw new IllegalArgumentException("Column type must be string");
				}
			}
		}
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"ExtractNumbers", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		return new NounMetadata(frame, PixelDataType.FRAME, opTypes);
	}

	private List<String> getColumns() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
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
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		boolean override = false;
		NounMetadata noun;
		if (grs != null) {
			noun = grs.getNoun(0);
			override = (Boolean) noun.getValue();
		}
		return override;
	}

}
