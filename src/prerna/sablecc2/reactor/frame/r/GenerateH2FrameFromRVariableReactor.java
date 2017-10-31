package prerna.sablecc2.reactor.frame.r;

import prerna.ds.h2.H2Frame;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.imports.H2Importer;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GenerateH2FrameFromRVariableReactor extends AbstractRFrameReactor {

	/**
	 * This reactor takes an r frame and synchronizes it to an h2 frame in
	 * semoss inputs are: 1) r data table name
	 */

	@Override
	public NounMetadata execute() {
		init();
		String varName = getVarName();
		this.rJavaTranslator.executeR(varName + " <- as.data.table(" + varName + ")");
		// recreate a new frame and set the frame name
		String[] colNames = this.rJavaTranslator.getColumns(varName);
		String[] colTypes = this.rJavaTranslator.getColumnTypes(varName);

		if (colNames == null || colTypes == null) {
			throw new IllegalArgumentException("Please make sure the variable " + varName + " exists and can be a valid data.table object");
		}

		// initialize new h2 frame
		H2Frame newTable = new H2Frame(varName);

		// generate the QS
		// set the column names and types
		CsvQueryStruct qs = new CsvQueryStruct();
		qs.setSelectorsAndTypes(colNames, colTypes);

		// we will make a temp file
		String tempFileLocation = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\"
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		tempFileLocation += "\\" + Utility.getRandomString(10) + ".csv";
		tempFileLocation = tempFileLocation.replace("\\", "/");
		this.rJavaTranslator.executeR("fwrite(" + varName + ", file='" + tempFileLocation + "')");

		// iterate through file and insert values
		qs.setCsvFilePath(tempFileLocation);
		H2Importer importer = new H2Importer(newTable, qs);
		// importer will create the necessary meta information
		importer.insertData();
		this.insight.setDataMaker(newTable);
		return new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	/**
	 * Get the input being the r variable name
	 * 
	 * @return
	 */
	private String getVarName() {
		return this.curRow.get(0).toString();
	}
}
