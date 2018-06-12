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

public class AutoCleanColumnReactor extends AbstractRFrameReactor {


	public AutoCleanColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String column = this.keyValue.get(this.keysToGet[0]);
		boolean keepCol = getKeepColBool();
		RDataTable table = (RDataTable) this.getFrame();
		String frame = table.getTableName();
		
		// check if packages are installed
		String[] packages = { "stringdist", "data.table", "tm", "cluster" };
		this.rJavaTranslator.checkPackages(packages);

		// make sure its a string
		String dataType = this.getColumnType(frame, column);
		if (SemossDataType.convertStringToDataType(dataType) != SemossDataType.STRING) {
			throw new IllegalArgumentException("The column type must be a String.");
		}

		// source teh scripts
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String sourceScript = "source(\"" + baseFolder + "\\R\\Recommendations\\master_col_data.r\") ;";
		sourceScript = sourceScript.replace("\\", "/");
		this.rJavaTranslator.runR(sourceScript);

		NounMetadata retNoun = new NounMetadata(table, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		OwlTemporalEngineMeta metaData = table.getMetaData();
		if (keepCol) {
			// new col is mastered version of column
			String tempCol = Utility.getRandomString(8);
			String newHeaderName = getCleanNewHeader(frame, column);
			String script = tempCol + " <- " + frame + "$" + column + ";" + 
					tempCol + " <- master_col_data(as.character("+ tempCol + "));" + 
					frame + " <- " + "cbind(" + frame + ", " + tempCol + ");" + 
					frame + "$"	+ newHeaderName + " <- " + tempCol + "; " + 
					frame + " <- " + frame + "[,-c('" + tempCol + "')];" +
					"rm(" + tempCol + ");";
			
			this.rJavaTranslator.runR(script);

			// add meta data to frame
			retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newHeaderName));
			metaData.addProperty(frame, frame + "__" + newHeaderName);
			metaData.setAliasToProperty(frame + "__" + newHeaderName, newHeaderName);
			metaData.setDataTypeToProperty(frame + "__" + newHeaderName, "STRING");
		} else {
			// execute the script on the column and replace original
			this.rJavaTranslator.runR(frame + "$" + column + " <- master_col_data(" + frame + "$" + column + ");");
		}
		table.syncHeaders();
		// garbage clean up
		this.rJavaTranslator.runR("gc();");

		return retNoun;
	}
	
	/**
	 * Create new column or override existing column
	 * @return
	 */
	private boolean getKeepColBool() {
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
