package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.poi.main.HeadersException;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.imports.ImportUtility;

public abstract class AbstractRFrameReactor extends AbstractFrameReactor {

	protected AbstractRJavaTranslator rJavaTranslator;

	/**
	 * This method must be called to initialize the rJavaTranslator
	 */
	protected void init() {
		this.rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		this.rJavaTranslator.startR(); 
	}

	/**
	 * This method is used to recreate the frame metadata
	 * when we execute a script that modifies the data structure
	 * @param frameName
	 */
	protected void recreateMetadata(String frameName) {
		// recreate a new frame and set the frame name
		String[] colNames = getColumns(frameName);
		String[] colTypes = getColumnTypes(frameName);
		RDataTable newTable = new RDataTable(frameName);
		ImportUtility.parseColumnsAndTypesToFlatTable(newTable, colNames, colTypes, frameName);
		this.insight.setDataMaker(newTable);
	}

	/**
	 * This method is used to fix the frame headers to be valid 
	 * @param frameName
	 * @param newColName
	 */
	protected String getCleanNewHeader(String frameName, String newColName) {
		// make the new column name valid
		HeadersException headerChecker = HeadersException.getInstance();
		String[] currentColumnNames = getColumns(frameName);
		String validNewHeader = headerChecker.recursivelyFixHeaders(newColName, currentColumnNames);
		return validNewHeader;
	}

	/**
	 * This method is used to get the column names of a frame
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		return this.rJavaTranslator.getColumns(frameName);
	}

	/**
	 * This method is used to get the column types of a frame
	 * @param frameName
	 */
	public String[] getColumnTypes(String frameName) {
		return this.rJavaTranslator.getColumnTypes(frameName);
	}

	/**
	 * This method is used to get the column type for a single column of a frame
	 * @param frameName
	 * @param column
	 */
	public String getColumnType(String frameName, String column) {
		return this.rJavaTranslator.getColumnType(frameName, column);
	}
	
	/**
	 * Change the frame column type
	 * @param frame
	 * @param frameName
	 * @param colName
	 * @param newType
	 * @param dateFormat
	 */
	public void changeColumnType(RDataTable frame, String frameName, String colName, String newType, String dateFormat) {
		this.rJavaTranslator.changeColumnType(frame, frameName, colName, newType, dateFormat);
	}
}
