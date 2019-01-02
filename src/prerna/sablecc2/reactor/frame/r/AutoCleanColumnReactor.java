package prerna.sablecc2.reactor.frame.r;

import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;

public class AutoCleanColumnReactor extends AbstractRFrameReactor {


	public AutoCleanColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String column = this.keyValue.get(this.keysToGet[0]);
		boolean override = overrideExistingColumn();
		RDataTable frame = (RDataTable) this.getFrame();
		String tableName = frame.getTableName();

		// check if packages are installed
		String[] packages = { "stringdist", "data.table", "tm", "cluster" };
		this.rJavaTranslator.checkPackages(packages);

		// make sure its a string
		String dataType = this.getColumnType(tableName, column);
		if (SemossDataType.convertStringToDataType(dataType) != SemossDataType.STRING) {
			throw new IllegalArgumentException("The column type must be a String.");
		}

		// source teh scripts
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String sourceScript = "source(\"" + baseFolder + "\\R\\Recommendations\\master_col_data.r\") ;";
		sourceScript = sourceScript.replace("\\", "/");
		this.rJavaTranslator.runR(sourceScript);

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		if(!override) {
			// new col is mastered version of column
			String tempCol = Utility.getRandomString(8);
			String newHeaderName = getCleanNewHeader(tableName, column);
			StringBuilder script = new StringBuilder();
			script.append(tempCol + " <- " + tableName + "$" + column + ";");
			script.append(tempCol + " <- master_col_data(as.character(" + tempCol + "));");
			script.append(tableName + " <- " + "cbind(" + tableName + ", " + tempCol + ");");
			script.append(tableName + "$" + newHeaderName + " <- " + tempCol + "; ");
			script.append(tableName + " <- " + tableName + "[,-c('" + tempCol + "')];");
			script.append("rm(" + tempCol + ");");
			this.rJavaTranslator.runR(script.toString());

			// add meta data to frame
			retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newHeaderName));
			metaData.addProperty(tableName, tableName + "__" + newHeaderName);
			metaData.setAliasToProperty(tableName + "__" + newHeaderName, newHeaderName);
			metaData.setDataTypeToProperty(tableName + "__" + newHeaderName, SemossDataType.STRING.toString());
		} else {
			// execute the script on the column and replace original
			this.rJavaTranslator.runR(tableName + "$" + column + " <- master_col_data(" + tableName + "$" + column + ");");
		}
		frame.syncHeaders();

		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				frame, 
				"AutoClean", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));

		return retNoun;
	}
	
	/**
	 * Create new column or override existing column
	 * @return
	 */
	private boolean overrideExistingColumn() {
		GenRowStruct boolGrs = this.store.getNoun(this.keysToGet[1]);
		if (boolGrs != null) {
			if (boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return true;
	}

}
