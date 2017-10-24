package prerna.sablecc2.reactor.frame.r.util;

import org.apache.log4j.Logger;

import prerna.ds.r.RDataTable;
import prerna.om.Insight;
import prerna.util.Utility;

public abstract class AbstractRJavaTranslator implements IRJavaTranslator {

	protected Insight insight = null;
	protected Logger logger = null;
	
	/**
	 * This method is used to get the column names of a frame
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		String script = "names(" + frameName + ");";
		String[] colNames = this.getStringArray(script);
		return colNames;
	}
	
	/**
	 * This method is used to get the column types of a frame
	 * @param frameName
	 */
	public String[] getColumnTypes(String frameName) {
		String script = "sapply(" + frameName + ", class);";
		String[] colTypes = this.getStringArray(script);
		return colTypes;
	}
	
	/**
	 * This method is used to get the column type for a single column of a frame
	 * @param frameName
	 * @param column
	 */
	public String getColumnType(String frameName, String column) {
		String script = "sapply(" + frameName + "$" + column + ", class);";
		String colType = this.getString(script);
		return colType;
	}
	
	public void changeColumnType(RDataTable frame, String frameName, String colName, String newType, String dateFormat) {
		String script = null;
		if (newType.equalsIgnoreCase("string")) {
			script = frameName + " <- " + frameName + "[, " + colName + " := as.character(" + colName + ")]";
			this.executeR(script);
		} else if (newType.equalsIgnoreCase("factor")) {
			script = frameName + " <- " + frameName + "[, " + colName + " := as.factor(" + colName + ")]";
			this.executeR(script);
		} else if (newType.equalsIgnoreCase("number")) {
			script = frameName + " <- " + frameName + "[, " + colName + " := as.numeric(" + colName + ")]";
			this.executeR(script);
		} else if (newType.equalsIgnoreCase("date")) {
			// we have a different script to run if it is a str to date
			// conversion
			// or a date to new date format conversion
			String type = this.getColumnType(frameName, colName);
			String tempTable = Utility.getRandomString(6);
			if (type.equalsIgnoreCase("date")) {
				String formatString = ", format = '" + dateFormat + "'";
				script = tempTable + " <- format(" + frameName + "$" + colName + formatString + ")";
				this.executeR(script);
				script = frameName + "$" + colName + " <- " + "as.Date(" + tempTable + formatString + ")";
				this.executeR(script);
			} else {
				script = tempTable + " <- as.Date(" + frameName + "$" + colName + ", format='" + dateFormat + "')";
				this.executeR(script);
				script = frameName + "$" + colName + " <- " + tempTable;
				this.executeR(script);
			}
			// perform variable cleanup
			this.executeR("rm(" + tempTable + ");");
			this.executeR("gc();");
		}
		System.out.println("Successfully changed data type for column = " + colName);
		frame.getMetaData().modifyDataTypeToProperty(frameName + "__" + colName, frameName, newType);
	}
	
	/**
	 * Get number of rows from an r script
	 * @param frameName
	 * @return
	 */
	public int getNumRows(String frameName) {
		String script = "nrow(" + frameName + ")";
		int numRows = this.getInt(script);
		return numRows;
	}
	
	/**
	 * This method is used to set the insight
	 * @param insight
	 */
	@Override
	public void setInsight(Insight insight) {
		this.insight = insight;
	}
	
	/**
	 * This method is used to set the insight
	 * @param logger
	 */
	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public void endR() {
		// TODO Auto-generated method stub
		
	}
}
