package prerna.sablecc2.reactor.frame.r.util;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.rosuda.JRI.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPGenericVector;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPList;
import org.rosuda.REngine.REXPString;

import prerna.ds.r.RDataTable;
import prerna.om.Insight;
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
	 * This method is used to set the insight
	 * 
	 * @param logger
	 */
	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public void getResultAsString(Object output, StringBuilder builder) {
		// Generic vector..
		if (output instanceof REXPGenericVector) {
			org.rosuda.REngine.RList list = ((REXPGenericVector) output).asList();

			String[] attributeNames = getAttributeArr(((REXPGenericVector) output)._attr());
			boolean matchesRows = false;
			// output list attribute names if present
			if (attributeNames != null) {
				// Due to the way R sends back data
				// When there is a list, it may contain a name label
				matchesRows = list.size() == attributeNames.length;
				if (!matchesRows) {
					if (attributeNames.length == 1) {
						builder.append("\n" + attributeNames[0] + "\n");
					} else if (attributeNames.length > 1) {
						builder.append("\n" + Arrays.toString(attributeNames) + "\n");
					}
				}
			}
			int size = list.size();
			for (int listIndex = 0; listIndex < size; listIndex++) {
				if (matchesRows) {
					builder.append("\n" + attributeNames[listIndex] + " : ");
				}
				getResultAsString(list.get(listIndex), builder);
			}
		}

		// List..
		else if (output instanceof REXPList) {
			org.rosuda.REngine.RList list = ((REXPList) output).asList();

			String[] attributeNames = getAttributeArr(((REXPList) output)._attr());
			boolean matchesRows = false;
			// output list attribute names if present
			if (attributeNames != null) {
				// Due to the way R sends back data
				// When there is a list, it may contain a name label
				matchesRows = list.size() == attributeNames.length;
				if (!matchesRows) {
					if (attributeNames.length == 1) {
						builder.append("\n" + attributeNames[0] + "\n");
					} else if (attributeNames.length > 1) {
						builder.append("\n" + Arrays.toString(attributeNames) + "\n");
					}
				}
			}
			int size = list.size();
			for (int listIndex = 0; listIndex < size; listIndex++) {
				if (matchesRows) {
					builder.append("\n" + attributeNames[listIndex] + " : ");
				}
				getResultAsString(list.get(listIndex), builder);
			}
		}

		// Integers..
		else if (output instanceof REXPInteger) {
			int[] ints = ((REXPInteger) output).asIntegers();
			if (ints.length > 1) {
				for (int intIndex = 0; intIndex < ints.length; intIndex++) {
					if (intIndex == 0) {
						builder.append(ints[intIndex]);
					} else {
						builder.append(" ").append(ints[intIndex]);
					}
				}
			} else {
				builder.append(ints[0]);
			}
		}

		// Doubles..
		else if (output instanceof REXPDouble) {
			double[] doubles = ((REXPDouble) output).asDoubles();
			if (doubles.length > 1) {
				for (int intIndex = 0; intIndex < doubles.length; intIndex++) {
					if (intIndex == 0) {
						builder.append(doubles[intIndex]);
					} else {
						builder.append(" ").append(doubles[intIndex]);
					}
				}
			} else {
				builder.append(doubles[0]);
			}
		}

		// Strings..
		else if (output instanceof REXPString) {
			String[] strings = ((REXPString) output).asStrings();
			if (strings.length > 1) {
				for (int intIndex = 0; intIndex < strings.length; intIndex++) {
					if (intIndex == 0) {
						builder.append(strings[intIndex]);
					} else {
						builder.append(" ").append(strings[intIndex]);
					}
				}
			} else {
				builder.append(strings[0]);
			}
		}

		// JRI
		else if (output instanceof org.rosuda.JRI.REXP) {
			int typeInt = ((org.rosuda.JRI.REXP) output).getType();
			if (typeInt == REXP.XT_DOUBLE) {
				builder.append(((org.rosuda.JRI.REXP) output).asDouble());

			} else if (typeInt == REXP.XT_ARRAY_DOUBLE) {
				builder.append(Arrays.toString(((org.rosuda.JRI.REXP) output).asDoubleArray()));

			} else if (typeInt == REXP.XT_ARRAY_DOUBLE + 1) {
				builder.append(Arrays.toString(((org.rosuda.JRI.REXP) output).asDoubleMatrix()));

			} else if (typeInt == REXP.XT_INT) {
				builder.append(((org.rosuda.JRI.REXP) output).asInt());

			} else if (typeInt == REXP.XT_ARRAY_INT) {
				builder.append(Arrays.toString(((org.rosuda.JRI.REXP) output).asIntArray()));

			} else if (typeInt == REXP.XT_STR) {
				builder.append(((org.rosuda.JRI.REXP) output).asString());

			} else if (typeInt == REXP.XT_ARRAY_STR) {
				builder.append(Arrays.toString(((org.rosuda.JRI.REXP) output).asStringArray()));

			} else if (typeInt == REXP.XT_BOOL) {
				builder.append(((org.rosuda.JRI.REXP) output).asBool());

			} else if (typeInt == REXP.XT_ARRAY_BOOL) {

			} else if (typeInt == REXP.XT_LIST) {
				builder.append(((org.rosuda.JRI.REXP) output).asString());

			} else if (typeInt == REXP.XT_VECTOR) {
				builder.append(Arrays.toString(((org.rosuda.JRI.REXP) output).asStringArray()));
			}
		}
		builder.append("\n");
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

}
