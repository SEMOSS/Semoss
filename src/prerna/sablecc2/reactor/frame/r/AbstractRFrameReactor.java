package prerna.sablecc2.reactor.frame.r;

import prerna.ds.r.RDataTable;
import prerna.poi.main.HeadersException;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.frame.r.util.IRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaTranslatorFactory;
import prerna.sablecc2.reactor.imports.ImportUtility;

public abstract class AbstractRFrameReactor extends AbstractFrameReactor {

	protected IRJavaTranslator rJavaTranslator;

	/**
	 * This method must be called to initialize the rJavaTranslator
	 */
	protected void init() {
		this.rJavaTranslator = RJavaTranslatorFactory.getRJavaTranslator(this.insight, this.getLogger(this.getClass().getName()));
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
		String script = "names(" + frameName + ");";
		String[] colNames = this.rJavaTranslator.getStringArray(script);
		return colNames;
	}

	/**
	 * This method is used to get the column types of a frame
	 * @param frameName
	 */
	public String[] getColumnTypes(String frameName) {
		String script = "sapply(" + frameName + ", class);";
		String[] colTypes = this.rJavaTranslator.getStringArray(script);
		return colTypes;
	}

	/**
	 * This method is used to get the column type for a single column of a frame
	 * @param frameName
	 * @param column
	 */
	public String getColumnType(String frameName, String column) {
		String script = "sapply(" + frameName + "$" + column + ", class);";
		String colType = this.rJavaTranslator.getString(script);
		return colType;
	}

	//	public IMetaData.DATA_TYPES getColEnum(String colName) {
	//		IMetaData.DATA_TYPES colTypeEnum = this.getFrame().getMetaData().getHeaderToTypeMap().get(colName);
	//		return colTypeEnum;  
	//		}

	//	public String getColType(String colName) {
	//		IMetaData.DATA_TYPES colTypeEnum = getColEnum(colName);
	//		String colType = ""; 
	//		if (colTypeEnum == IMetaData.DATA_TYPES.STRING) {
	//			colType = "string"; 
	//		} else if (colTypeEnum == IMetaData.DATA_TYPES.NUMBER) {
	//			colType = "number"; 
	//		} else if (colTypeEnum == IMetaData.DATA_TYPES.DATE) {
	//			colType = "date"; 
	//		}
	//		return colType; 
	//	}

	//	public String[] getColTypes() {
	//		ArrayList<IMetaData.DATA_TYPES> colTypes = new ArrayList<IMetaData.DATA_TYPES>();
	//		for (IMetaData.DATA_TYPES colType : this.getFrame().getMetaData().getHeaderToTypeMap().values()) {
	//			colTypes.add(colType);
	//		}
	//		String[] colTypesString = new String[colTypes.size()]; 
	//		for (int i = 0; i < colTypes.size(); i++) {
	//			if (colTypes.get(i) == IMetaData.DATA_TYPES.STRING) {
	//				colTypesString[i] = "string"; 
	//			} else if (colTypes.get(i) == IMetaData.DATA_TYPES.NUMBER) {
	//				colTypesString[i] = "number"; 
	//			} else if (colTypes.get(i) == IMetaData.DATA_TYPES.DATE) {
	//				colTypesString[i] = "date"; 
	//			}
	//		}
	//		return colTypesString;
	//	}
}
