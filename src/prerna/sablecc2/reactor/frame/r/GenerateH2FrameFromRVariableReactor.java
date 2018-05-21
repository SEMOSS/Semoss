package prerna.sablecc2.reactor.frame.r;

import prerna.ds.h2.H2Frame;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.imports.H2Importer;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GenerateH2FrameFromRVariableReactor extends AbstractRFrameReactor {

	/**
	 * This reactor takes an r frame and synchronizes it to an h2 frame in
	 * semoss inputs are: 1) r data table name
	 */
	
	public GenerateH2FrameFromRVariableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		// get rFrameName
		String varName = getVarName();
		H2Frame newTable = new H2Frame(varName);

		//sync R dataframe to H2Frame
		syncFromR(this.rJavaTranslator,varName, newTable);
		
		this.insight.setDataMaker(newTable);
		return new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	/**
	 * 
	 * @param rFrameName
	 * @param frame
	 */
	public void syncFromR(AbstractRJavaTranslator rJavaTranslator, String rFrameName, H2Frame frame) {
		// generate the QS
		// set the column names and types
		rJavaTranslator.executeR(rFrameName + " <- as.data.table(" + rFrameName + ")");
		// recreate a new frame and set the frame name
		String[] colNames = rJavaTranslator.getColumns(rFrameName);
		String[] colTypes = rJavaTranslator.getColumnTypes(rFrameName);

		if (colNames == null || colTypes == null) {
			throw new IllegalArgumentException("Please make sure the variable " + rFrameName + " exists and can be a valid data.table object");
		}
		
		CsvQueryStruct qs = new CsvQueryStruct();
		qs.setSelectorsAndTypes(colNames, colTypes);

		// we will make a temp file
		String tempFileLocation = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		tempFileLocation += "\\" + Utility.getRandomString(10) + ".csv";
		tempFileLocation = tempFileLocation.replace("\\", "/");
		rJavaTranslator.executeR("fwrite(" + rFrameName + ", file='" + tempFileLocation + "')");

		// iterate through file and insert values
		qs.setFilePath(tempFileLocation);
		H2Importer importer = new H2Importer(frame, qs);
		// importer will create the necessary meta information
		importer.insertData();
	}
	
	/**
	 * Get the input being the r variable name
	 * 
	 * @return
	 */
	private String getVarName() {
		return this.curRow.get(0).toString();
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.VARIABLE.getKey())) {
			return "Name of the r variable";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
