package prerna.sablecc2.reactor.frame.r.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.rosuda.REngine.REXPList;
import org.rosuda.REngine.REXPString;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.util.Utility;

public abstract class AbstractRJavaTranslator implements IRJavaTranslator {

	protected Insight insight = null;
	protected Logger logger = null;

	/**
	 * This method is used to get the column names of a frame
	 * 
	 * @param frameName
	 */
	public String[] getColumns(String frameName) {
		String script = "names(" + frameName + ");";
		String[] colNames = this.getStringArray(script);
		return colNames;
	}

	/**
	 * This method is used to get the column types of a frame
	 * 
	 * @param frameName
	 */
	public String[] getColumnTypes(String frameName) {
		String script = "sapply(" + frameName + ", class);";
		String[] colTypes = this.getStringArray(script);
		return colTypes;
	}

	/**
	 * This method is used to get the column type for a single column of a frame
	 * 
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
	 * 
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
	 * 
	 * @param insight
	 */
	@Override
	public void setInsight(Insight insight) {
		this.insight = insight;
	}

	/**
	 * This method is used to set the logger
	 * 
	 * @param logger
	 */
	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public String[] getAttributeArr(REXPList attrList) {
		if (attrList == null) {
			return null;
		}
		if (attrList.length() > 0) {
			Object attr = attrList.asList().get(0);
			if (attr instanceof REXPString) {
				String[] strAttr = ((REXPString) attr).asStrings();
				return strAttr;
			}
		}
		return null;
	}
	
	/**
	 * This method is used initialize an empty matrix with the appropriate
	 * number of rows and columns
	 * @param matrix
	 * @param numRows
	 * @param numColumns
	 */
	public void initEmptyMatrix(List<Object[]> matrix, int numRows, int numCols) {
		for(int i = 0; i < numRows; i++) {
			matrix.add(new Object[numCols]);
		}
	}
	
	/**
	 * This method is used generate a r data.table from a given query
	 * Returns the r variable name that references the created data.table
	 * @param frame
	 * @param qs
	 * @return rFrame name
	 */
	public String generateRDataTableVariable(ITableDataFrame frame, QueryStruct2 qs) {
		String dfName = "f_" + Utility.getRandomString(10);

		// use an iterator to get the instance values from the qs
		// we will use these instance values to construct a new r data frame
		Iterator<IHeadersDataRow> it = (Iterator<IHeadersDataRow>) frame.query(qs);
		
		// these stringbuilders will build the new r table and populate the
		// table with the instance values
		StringBuilder instanceValuesBuilder = new StringBuilder(); // this puts values into the frame we are constructing

		// build instance list
		List<Object[]> instanceList = new ArrayList<Object[]>();
		while (it.hasNext()) {
			Object[] values = it.next().getRawValues();
			instanceList.add(values);
		}

		// now that we have the instance values, we can use them to build a string to populate the table that we will make in r
		// colNameString will keep track of the columns we are creating in our new r data frame
		List<IQuerySelector> inputSelectors = qs.getSelectors();
		int numSelectors = inputSelectors.size();
		StringBuilder colNameSb = new StringBuilder();
		for (int i = 0; i < numSelectors; i++) {
			// use the column name without the frame name
			colNameSb.append(inputSelectors.get(i).getAlias());
			colNameSb.append("= character()").append(",");
			for (int j = 0; j < instanceList.size(); j++) {
				instanceValuesBuilder.append(dfName + "[" + (j + 1) + "," + (i + 1) + "]");
				instanceValuesBuilder.append("<-");
				if (instanceList.get(j) == null) {
					instanceValuesBuilder.append("\"" + "" + "\"");
				} else {
					// replace underscores with spaces in the instance data
					instanceValuesBuilder.append("\"" + instanceList.get(j)[i].toString().replaceAll("_", " ") + "\"");
				}
				instanceValuesBuilder.append(";");
			}
		}

		// colNameString format: Title= character(),Genre= character()
		String colNameString = colNameSb.substring(0, colNameSb.length()-1);
		// scriptSb format: frame<-data.frame(Title= character(), stringsAsFactors = FALSE); + the instances
		StringBuilder scriptSb = new StringBuilder(); // this builds the dataframe
		scriptSb.append(dfName).append("<-data.frame(").append(colNameString).append(", stringsAsFactors = FALSE);")
			.append(instanceValuesBuilder.toString());
		// run the total script
		this.runR(scriptSb.toString());
		return dfName;
	}

	/**
	 * Check if r packages are install throw an error is an r package is missing
	 * 
	 * @param packages
	 */
    public void checkPackages(String[] packages) {
		String packageError = "";
		for (String rPackage : packages) {
			String hasPackage = this.getString("as.character(\"" + rPackage + "\" %in% rownames(installed.packages()))");
			if (!hasPackage.equalsIgnoreCase("true")) {
				packageError += rPackage + "\n";
			}
		}
		if (packageError.length() > 0) {
			String errorMessage = "\nMake sure you have all the following R libraries installed:\n" + packageError;
			throw new IllegalArgumentException(errorMessage);
		}

    }
}
